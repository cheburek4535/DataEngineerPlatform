package kafka.integration.models;

import java.math.BigDecimal;

public class Currency {
    public record CurrencyRaw(
            String code,
            CurrencyData data
    ) {}
    public record CurrencyData(
            String name,
            BigDecimal value,
            BigDecimal nominal
    ) {}
    public record CurrencyStructured(
            String code,
            String name,
            BigDecimal value_in_rubles
    ) {}
}
