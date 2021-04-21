package dynamodb.idempotency.poc;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDate;
import java.util.Objects;


@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class UtilizacaoSaldo {

    private String transacaoId;
    private String cliente;
    private String data;
    private String valorUtilizado;

    @Override
    public int hashCode() {
        return Objects.hash(transacaoId, cliente, data, valorUtilizado);
    }
}
