package dynamodb.idempotency.poc;

import io.micronaut.configuration.picocli.PicocliRunner;
import io.micronaut.context.BeanContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(name = "Consolidador")
public class Application implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        PicocliRunner.run(Application.class, args);
    }

    public void run() {
        logger.info("iniciando...");
        Consolidador consolidador = BeanContext.run().getBean(Consolidador.class);
        consolidador.iniciar();
        logger.info("processamento completo");
    }
}
