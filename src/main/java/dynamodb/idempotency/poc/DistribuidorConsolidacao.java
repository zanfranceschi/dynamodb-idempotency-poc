package dynamodb.idempotency.poc;

import io.micronaut.context.BeanContext;
import io.micronaut.scheduling.annotation.Scheduled;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.FileInputStream;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;

@Singleton
public class DistribuidorConsolidacao {

    private static final Logger logger = LoggerFactory.getLogger(DistribuidorConsolidacao.class);

    private static final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();

    public ConcurrentLinkedQueue<String> getQueue() {
        return queue;
    }

    @SneakyThrows
    @Scheduled(initialDelay = "1s")
    public void iniciar() {

        int workers = 5;
        logger.info(String.format("Iniciando %s consolidadores", workers));

        try (BeanContext context = BeanContext.run()) {
            for (int i = 0; i < workers; i++) {
                Consolidador consolidador = context.getBean(Consolidador.class);
                new Thread(consolidador::iniciar).start();
            }
        }

        String arquivoSaldosCaminho = getClass().getClassLoader().getResource("saldos.txt").getPath(); // que rolê pra achar um caminho, hein?!
        FileInputStream fis = new FileInputStream(arquivoSaldosCaminho);
        Scanner sc = new Scanner(fis);

        while (sc.hasNextLine()) {
            String linha = sc.nextLine();
            queue.add(linha);
        }
    }
}
