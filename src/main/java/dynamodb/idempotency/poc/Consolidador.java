package dynamodb.idempotency.poc;

import io.micronaut.context.event.StartupEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.net.URI;
import java.util.*;

import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import javax.inject.Singleton;

@Singleton
public class Consolidador {

    private static final Logger logger = LoggerFactory.getLogger(Consolidador.class);

    private DynamoDbClient dynamoDbClient;

    public Consolidador() {
        /*
             Para criar uma tabela usando o DynamoDB localmente com o aws cli:

             aws dynamodb create-table \
                --table-name UtilizacaoSaldo \
                --attribute-definitions AttributeName=transacaoId,AttributeType=S \
                --key-schema AttributeName=transacaoId,KeyType=HASH \
                --provisioned-throughput ReadCapacityUnits=100,WriteCapacityUnits=100 \
                --endpoint-url http://localhost:8000
        */

        dynamoDbClient = DynamoDbClient.builder()
                // Usar o DynamoDB localmente
                // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
                .endpointOverride(URI.create("http://localhost:8000"))
                .build();
    }

    @SneakyThrows
    @EventListener
    public void iniciar(StartupEvent e) {

        String arquivoSaldosCaminho = getClass().getClassLoader().getResource("saldos.txt").getPath();
        FileInputStream fis = new FileInputStream(arquivoSaldosCaminho);

        Scanner sc = new Scanner(fis);

        while (sc.hasNextLine()) {

            logger.info(gerarLinhaMock());

            String linha = sc.nextLine();

            // Arquivo posicional
            String transacaoId = linha.substring(0, 3).trim();
            String data = linha.substring(3, 10).trim();
            String cliente = linha.substring(11, 25).trim();
            String saldoUtilizado = linha.substring(26, 30).trim();

            // Instancia o objeto para poder obter o hashCode
            UtilizacaoSaldo utilizacaoSaldo = new UtilizacaoSaldo(transacaoId, cliente, data, saldoUtilizado);

            // POJO para Registro DynamoDB
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("transacaoId", AttributeValue.builder().s(utilizacaoSaldo.getTransacaoId()).build());
            item.put("cliente", AttributeValue.builder().s(utilizacaoSaldo.getCliente()).build());
            item.put("data", AttributeValue.builder().s(utilizacaoSaldo.getData()).build());
            item.put("valorUtilizado", AttributeValue.builder().s(utilizacaoSaldo.getValorUtilizado()).build());
            item.put("hashCode", AttributeValue.builder().n(String.valueOf(utilizacaoSaldo.hashCode())).build());

            // Parâmetro para a Condition Expression (hashCode <> objeto.hashCode())
            Map<String, AttributeValue> attrValues = new HashMap<>();
            attrValues.put(":hashCode", AttributeValue.builder().n(String.valueOf(utilizacaoSaldo.hashCode())).build());

            try {
                PutItemResponse response = dynamoDbClient.putItem(
                        PutItemRequest.builder()
                                .tableName("UtilizacaoSaldo")
                                .item(item)
                                // ALL_OLD faz com que atualizações retornem os valores anteriores
                                .returnValues("ALL_OLD")
                                // se a condição falhar, uma ConditionalCheckFailedException é lançada
                                .conditionExpression("hashCode <> :hashCode")
                                .expressionAttributeValues(attrValues)
                                .build());

                if (response.attributes().size() == 0) {
                    logger.info("registro criado: " + linha);
                } else {
                    logger.info("registro atualziado: " + linha);
                }
            } catch (ConditionalCheckFailedException cex) {
                logger.info("registro idêntico existente: ignorado!: " + linha);
            }
        }
    }

    public static String gerarLinhaMock() {
        Random rand = new Random();
        String transacaoId = String.format("%03d", rand.nextInt(999));
        String data = String.format("%08d", rand.nextInt(99999999));

        return transacaoId + " " + data;
    }
}