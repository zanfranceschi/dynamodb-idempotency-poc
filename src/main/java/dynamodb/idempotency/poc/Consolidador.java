package dynamodb.idempotency.poc;


import io.micronaut.context.annotation.Prototype;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;


import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@Prototype
public class Consolidador {

    private static final Logger logger = LoggerFactory.getLogger(Consolidador.class);
    private DynamoDbClient dynamoDbClient;
    private final DistribuidorConsolidacao distribuidorConsolidacao;

    public Consolidador(DistribuidorConsolidacao distribuidorConsolidacao) {

        this.distribuidorConsolidacao = distribuidorConsolidacao;

        dynamoDbClient = DynamoDbClient.builder()
                // Usar o DynamoDB localmente
                // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
                .endpointOverride(URI.create("http://localhost:8000"))
                .build();
    }

    @SneakyThrows
    public void iniciar() {

        ConcurrentLinkedQueue<String> queue = distribuidorConsolidacao.getQueue();

        while (true) {

            String linha = queue.poll();

            if (linha == null) {
                Thread.sleep(100);
                continue;
            }

            String transacaoId = linha.substring(0, 2).trim();
            String cliente = linha.substring(10, 24).trim();
            String data = linha.substring(2, 9).trim();
            String saldoUtilizado = linha.substring(25, 29).trim();

            UtilizacaoSaldo utilizacaoSaldo = new UtilizacaoSaldo(transacaoId, cliente, data, saldoUtilizado);

            Map<String, AttributeValue> item = new HashMap<>();
            item.put("transacaoId", AttributeValue.builder().s(utilizacaoSaldo.getTransacaoId()).build());
            item.put("cliente", AttributeValue.builder().s(utilizacaoSaldo.getCliente()).build());
            item.put("data", AttributeValue.builder().s(utilizacaoSaldo.getData()).build());
            item.put("valorUtilizado", AttributeValue.builder().s(utilizacaoSaldo.getValorUtilizado()).build());
            item.put("hashCode", AttributeValue.builder().n(String.valueOf(utilizacaoSaldo.hashCode())).build());

            Map<String, AttributeValue> attrValues = new HashMap<>();

            attrValues.put(":hashCode", AttributeValue.builder().n(String.valueOf(utilizacaoSaldo.hashCode())).build());

            try {
                PutItemResponse response = dynamoDbClient.putItem(
                        PutItemRequest.builder()
                                .tableName("UtilizacaoSaldo")
                                .item(item)
                                .returnValues("ALL_OLD")
                                .expressionAttributeValues(attrValues)
                                .conditionExpression("hashCode <> :hashCode")
                                .build());

                if (response.attributes().size() == 0) {
                    logger.info("registro criado: " + linha);
                } else {
                    logger.info("registro atualziado: " + linha);
                }
            }
            catch (ConditionalCheckFailedException cex) {
                logger.info("registro idêntico existente: ignorado!: " + linha);
            }
        }
    }
}
