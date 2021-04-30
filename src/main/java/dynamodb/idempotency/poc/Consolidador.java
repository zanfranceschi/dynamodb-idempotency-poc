package dynamodb.idempotency.poc;

import io.micronaut.context.event.StartupEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import javax.inject.Singleton;

@Singleton
public class Consolidador {

    private static final Logger logger = LoggerFactory.getLogger(Consolidador.class);

    private DynamoDbClient dynamoDbClient;
    private S3Client s3Client;

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

        s3Client = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(ProfileCredentialsProvider.create("zanfranceschi"))
                .build();
    }

    @SneakyThrows
    @EventListener
    public void iniciar(StartupEvent e) {

        GetObjectRequest s3Request = GetObjectRequest.builder()
                .key("saldos.txt")
                .bucket("zanfranceschi")
                .build();

        ResponseInputStream<GetObjectResponse> s3responseIS = s3Client.getObject(s3Request);
        BufferedReader reader = new BufferedReader(new InputStreamReader(s3responseIS));

        String linhaS3 = null;

        while ((linhaS3 = reader.readLine()) != null) {
            logger.info("linha: " + linhaS3);
        }

        String arquivoSaldosCaminho = getClass().getClassLoader().getResource("saldos.txt").getPath();
        FileInputStream fis = new FileInputStream(arquivoSaldosCaminho);

        Scanner sc = new Scanner(fis);

        while (sc.hasNextLine()) {

            String linha = sc.nextLine();

            // Arquivo posicional
            String transacaoId = linha.substring(0, 3).trim();
            String data = linha.substring(3, 11).trim();
            String cliente = linha.substring(11, 26).trim();
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
                    logger.info("registro criado: " + utilizacaoSaldo);
                } else {
                    logger.info("registro atualziado: " + utilizacaoSaldo);
                }
            } catch (ConditionalCheckFailedException cex) {
                logger.info("registro idêntico existente: ignorado!: " + utilizacaoSaldo);
            }
        }
    }
}