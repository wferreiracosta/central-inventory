# Central Inventory — Arquitetura Distribuída de Estoque com Kafka e Spring Boot

## Resumo

O **Central Inventory** é uma solução de gestão de estoque distribuído orientada a eventos. O objetivo do projeto é centralizar e manter a consistência do inventário de produtos a partir de operações enviadas por sistemas externos (como backends de lojas após vendas, devoluções ou entradas de mercadoria), garantindo **desacoplamento**, **alta disponibilidade** e **processamento assíncrono**.

Em vez de os sistemas das lojas precisarem se conectar diretamente ao broker de mensageria ou lidar com a concorrência da base central, eles enviam comandos HTTP simples (`create`, `increment` ou `decrement`) para um serviço produtor, que valida e publica os eventos no **Apache Kafka**, enquanto um serviço consumidor processa essas mensagens e atualiza o banco de dados central respeitando regras de domínio (como impedir saldo negativo).

A solução é composta por dois microsserviços independentes em **Java 17** e **Spring Boot 3.1.5**, estruturados com **Arquitetura Hexagonal (Ports & Adapters)**:

1. **[`central-inventory-producer`](https://github.com/wferreiracosta/central-inventory-producer)**: API REST que atua como porta de entrada, valida requisições de ajuste de estoque e publica eventos no tópico Kafka.
2. **[`central-inventory-consumer`](https://github.com/wferreiracosta/central-inventory-consumer)**: Consumidor Kafka que lê os eventos de ajuste, aplica as regras de negócio de inventário e persiste o estado dos produtos em banco de dados relacional via JPA.

---

## Visão Geral da Arquitetura e Fluxo

```text
[Cliente / Loja]
      │
      │ POST /api/inventory/adjust (HTTP 202 Accepted)
      ▼
┌──────────────────────────────────────────┐
│        central-inventory-producer        │  (Porta 8080)
│  Controller -> InventoryService -> Kafka │
└──────────────────────────────────────────┘
      │
      │ Tópico: central-inventory-adjust
      │ Chave: sku | Valor: JSON (EventDTO)
      ▼
┌──────────────────────────────────────────┐
│            Apache Kafka (KRaft)          │  (Porta 9092)
│         + Kafdrop UI (Porta 19000)       │
└──────────────────────────────────────────┘
      │
      │ @KafkaListener (group-id: central-inventory-group)
      ▼
┌──────────────────────────────────────────┐
│        central-inventory-consumer        │  (Porta 8081)
│  Consumer -> InventoryService -> JPA/H2  │
└──────────────────────────────────────────┘
```

### Operações Suportadas (`OperationEnum`)

| Operação | Valor JSON | Comportamento no Consumer |
| :--- | :--- | :--- |
| Criação | `"create"` | Cria o produto caso o `sku` ainda não exista na base (rejeita quantidade inicial negativa). Se já existir, ignora com log de aviso. |
| Incremento | `"increment"` | Soma `quantity` ao estoque existente do `sku` (rejeita valor negativo). Se o `sku` não existir, registra aviso no log. |
| Decremento | `"decrement"` | Subtrai `quantity` do estoque existente do `sku` (rejeita valor negativo). Nunca deixa o estoque negativo (zera caso `quantity >= estoque atual`). |

---

## Análise dos Projetos

### 1. [`central-inventory-producer`](https://github.com/wferreiracosta/central-inventory-producer)

Responsável por desacoplar os sistemas clientes da infraestrutura do Kafka.

- **Repositório:** https://github.com/wferreiracosta/central-inventory-producer
- **Porta HTTP:** `8080` (padrão do Spring Boot)
- **Estrutura Hexagonal:**
  - `domain/model`: `OperationEnum` (`CREATE`, `INCREMENT`, `DECREMENT` serializados como `"create"`, `"increment"`, `"decrement"`).
  - `application/dto`: `OperationRequest`, `OperationResponse` e `EventDTO`.
  - `application/usecase`: interface `InventoryUseCase`.
  - `application/port/out`: interface `InventoryProducerPort`.
  - `application/service`: `InventoryService` (converte a requisição em `EventDTO` e trata sucesso/erro de forma assíncrona via `CompletableFuture`).
  - `infrastructure/adapter/in/web`:
    - `InventoryController`: expõe `POST /api/inventory/adjust`, retornando `202 Accepted` com `OperationResponse`.
    - `GlobalExceptionHandler`: trata `MethodArgumentNotValidException` retornando `400 Bad Request` no formato `ValidationError` (`status`, `message`, `timestamp`, `errors`).
  - `infrastructure/adapter/out/message`: `InventoryProducerAdapter`, que envia o `EventDTO` para o tópico `${app.kafka.topic.adjust}` (`central-inventory-adjust`) usando `KafkaTemplate<String, Object>` e o `sku` como chave da mensagem.
  - `infrastructure/config`: `ApplicationProperties` (`info.app.*`) e `SwaggerConfig` (OpenAPI/Swagger UI).
- **Tecnologias:** Java 17, Spring Boot 3.1.5, Spring Web, Spring Kafka, Spring Validation, Springdoc OpenAPI (`2.5.0`), Lombok, Embedded Kafka (testes).

#### Contrato da API

**Endpoint:** `POST http://localhost:8080/api/inventory/adjust`

**Request (`OperationRequest`):**
```json
{
  "sku": "12345",
  "quantity": 10,
  "operation": "create"
}
```
> Todos os campos (`sku`, `quantity`, `operation`) possuem `@NotNull`.

**Response (`202 Accepted` — `OperationResponse`):**
```json
{
  "sent": true,
  "errorMessage": null,
  "timestamp": "2026-11-22T16:14:29.000+00:00"
}
```

**Swagger UI:** `http://localhost:8080/swagger-ui.html`

---

### 2. [`central-inventory-consumer`](https://github.com/wferreiracosta/central-inventory-consumer)

Responsável por consumir os eventos do Kafka e manter o saldo dos produtos atualizado na tabela `products`.

- **Repositório:** https://github.com/wferreiracosta/central-inventory-consumer
- **Porta HTTP:** `8081` (`server.port=8081`)
- **Estrutura Hexagonal:**
  - `domain/model`: entidade de domínio `Product` (regras de criação, incremento, decremento e trava de estoque não negativo) e `OperationEnum`.
  - `application/dto`: `EventDTO(Long sku, Long quantity, OperationEnum operation)`.
  - `application/port/in`: interface `InventoryUseCase`.
  - `application/port/out`: interface `ProductPort`.
  - `application/service`: `InventoryService` (anotado com `@Transactional`, executa criação, incremento ou decremento conforme a operação).
  - `infrastructure/adapter/in/consumer`: `InventoryConsumer` com `@KafkaListener(topics = "${app.kafka.topic.adjust}")` e `@Valid EventDTO`.
  - `infrastructure/adapter/out/persistence`: `ProductPersistenceAdapter`, `ProductRepository` (`JpaRepository`) e `ProductEntity` (tabela `products`).
  - `infrastructure/config`: `KafkaConfig` com `DefaultErrorHandler` e `FixedBackOff(1000L, 3)` (3 tentativas com intervalo de 1 segundo em caso de falha).
- **Configuração Kafka (`application.properties`):**
  - Tópico: `central-inventory-adjust`
  - Consumer Group: `central-inventory-group`
  - Desserialização: `JsonDeserializer` com `spring.json.trusted.packages=*`, tipo padrão `EventDTO` e `accept-case-insensitive-enums=true`.
- **Configuração de Banco (`application.properties`):**
  - H2 em memória: `jdbc:h2:mem:central-inventory-database` (usuário `sa`, senha vazia)
  - Console H2 habilitado em `/h2-console` (`http://localhost:8081/h2-console`)
- **Tecnologias:** Java 17, Spring Boot 3.1.5, Spring Web, Spring Kafka, Spring Data JPA, Spring Validation, Springdoc OpenAPI (`2.5.0`), Lombok, Awaitility e Embedded Kafka (testes).

---

## Pontos de Atenção Antes de Executar

Ao analisar os arquivos de configuração e código-fonte, há alguns detalhes importantes para evitar erros na execução:

1. **Dependência do H2 ausente no `pom.xml` do Consumer:**
   O `application.properties` do `central-inventory-consumer` configura `org.h2.Driver` e o console H2, porém a dependência `com.h2database:h2` **não está declarada** no `central-inventory-consumer/pom.xml`. Para rodar a aplicação ou os testes de integração localmente sem alterar para outro banco, adicione no `pom.xml` do consumer:
   ```xml
   <dependency>
       <groupId>com.h2database</groupId>
       <artifactId>h2</artifactId>
       <scope>runtime</scope>
   </dependency>
   ```
2. **Docker Compose duplicado:**
   Ambos os projetos possuem um `docker-compose.yaml` idêntico que sobe os containers `central-inventory-kafka` (porta `9092`, modo KRaft sem Zookeeper) e `central-inventory-kafdrop` (porta `19000`). **Suba o Docker Compose em apenas uma das pastas** para evitar conflito de nomes e portas.
3. **Tipo do campo `sku` entre Producer e Consumer:**
   No `producer`, `sku` é `String`; no `consumer`, `EventDTO.sku` e `ProductEntity.sku` são `Long`/`long`. Portanto, envie apenas **SKUs numéricos** (ex.: `"12345"`), caso contrário a desserialização falhará no consumer.
4. **Porta do H2 Console:**
   O consumer sobe na porta **`8081`** (e não na `8080` mencionada no README interno do consumer). O console fica em `http://localhost:8081/h2-console`.

---

## Como Executar

### Pré-requisitos

- **Java 17+** (JDK)
- **Docker** e **Docker Compose**
- Maven já incluso via wrapper (`./mvnw` no Linux/macOS ou `mvnw.cmd` no Windows)

### Passo 1: Subir o Kafka e o Kafdrop

A partir da raiz de **um** dos projetos (por exemplo, `central-inventory-producer`):

```bash
cd central-inventory-producer
docker compose up -d
```

Verifique se os serviços estão ativos:
- **Kafka Broker:** `localhost:9092`
- **Kafdrop (UI Web):** `http://localhost:19000`

### Passo 2: Iniciar o Consumer

Em um terminal, entre na pasta do consumer (após garantir que o driver H2 está presente no `pom.xml`):

**Linux/macOS:**
```bash
cd central-inventory-consumer
./mvnw spring-boot:run
```

**Windows (PowerShell/CMD):**
```powershell
cd central-inventory-consumer
.\mvnw.cmd spring-boot:run
```

- Aplicação disponível na porta **`8081`**.
- Console H2: `http://localhost:8081/h2-console`
  - **JDBC URL:** `jdbc:h2:mem:central-inventory-database`
  - **User Name:** `sa`
  - **Password:** *(em branco)*

### Passo 3: Iniciar o Producer

Em outro terminal, entre na pasta do producer:

**Linux/macOS:**
```bash
cd central-inventory-producer
./mvnw spring-boot:run
```

**Windows (PowerShell/CMD):**
```powershell
cd central-inventory-producer
.\mvnw.cmd spring-boot:run
```

- API disponível na porta **`8080`**.
- Swagger UI: `http://localhost:8080/swagger-ui.html`

---

## Testando o Fluxo Completo (End-to-End)

1. **Criar um produto (`create`)** com SKU `100` e quantidade inicial `10`:
   ```bash
   curl -X POST http://localhost:8080/api/inventory/adjust \
     -H "Content-Type: application/json" \
     -d '{"sku": "100", "quantity": 10, "operation": "create"}'
   ```

2. **Incrementar estoque (`increment`)** em `5` unidades (saldo esperado: `15`):
   ```bash
   curl -X POST http://localhost:8080/api/inventory/adjust \
     -H "Content-Type: application/json" \
     -d '{"sku": "100", "quantity": 5, "operation": "increment"}'
   ```

3. **Decrementar estoque (`decrement`)** em `3` unidades (saldo esperado: `12`):
   ```bash
   curl -X POST http://localhost:8080/api/inventory/adjust \
     -H "Content-Type: application/json" \
     -d '{"sku": "100", "quantity": 3, "operation": "decrement"}'
   ```

4. **Consultar o resultado:**
   - No **Kafdrop** (`http://localhost:19000`), inspecione as mensagens publicadas no tópico `central-inventory-adjust`.
   - No **H2 Console** (`http://localhost:8081/h2-console`), execute:
     ```sql
     SELECT * FROM PRODUCTS;
     ```

---

## Executando os Testes Automatizados

Ambos os projetos usam `@EmbeddedKafka`, não dependendo do Docker para rodar os testes (lembrando apenas da dependência H2 no `pom.xml` do consumer).

```bash
# Producer
cd central-inventory-producer
./mvnw test   # ou .\mvnw.cmd test no Windows

# Consumer
cd central-inventory-consumer
./mvnw test   # ou .\mvnw.cmd test no Windows
```

---

## Encerrando o Ambiente

Para parar e remover os containers do Kafka e Kafdrop (na mesma pasta onde executou `up`):

```bash
docker compose down
```
