package dynamodb.idempotency.poc;

import io.micronaut.context.event.StartupEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;


@Singleton
public class Consolidador {

    private static final Logger logger = LoggerFactory.getLogger(Consolidador.class);
    private DynamoDbClient dynamoDbClient;

    public Consolidador(DynamoDbClientBuilder dynamoDbClientBuilder) {
        dynamoDbClient = dynamoDbClientBuilder
                // Usar o DynamoDB localmente
                // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
                .endpointOverride(URI.create("http://localhost:8000"))
                .build();
        ListTablesResponse response = dynamoDbClient.listTables();

        for (String table: response.tableNames()) {
            logger.info(String.format("table: %s", table));
        }
    }

    @EventListener
    public void run(StartupEvent e) {
        try {
            String arquivoSaldosCaminho = getClass().getClassLoader().getResource("saldos.txt").getPath(); // que rolê pra achar um caminho, hein?!
            FileInputStream fis = new FileInputStream(arquivoSaldosCaminho);

            Scanner sc = new Scanner(fis);

            List<UtilizacaoSaldo> utilizacoes = new ArrayList<>();

            logger.info("data    | nome           | saldoUtilizado");
            logger.info("--------+----------------+----------------");

            while (sc.hasNextLine()) {
                String linha = sc.nextLine();


                String transacaoId = linha.substring(0, 2).trim();
                String cliente = linha.substring(10, 24).trim();
                String data = linha.substring(2, 9).trim();
                String saldoUtilizado = linha.substring(25, 29).trim();

                UtilizacaoSaldo utilizacaoSaldo = new UtilizacaoSaldo(transacaoId, cliente, data, saldoUtilizado);

                logger.info(String.format("%s | %s | %s", data, cliente, saldoUtilizado));

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
                        logger.info("registro criado");
                    } else {
                        logger.info("registro atualziado");
                    }
                }
                catch (ConditionalCheckFailedException cex) {
                    logger.info("registro idêntico existente: ignorado!");
                }
            }

            logger.info("--------+----------------+----------------");

            sc.close();

        } catch (IOException ex) {
            logger.error("erro ao ler o arquivo", ex);
        }
    }

}
