package kafka.integration.models;

import java.math.BigDecimal;

public class Currency {
    public record CurrencyStructured(
            String code,
            String name,
            BigDecimal valueInRubles
    ) {}
    public record SharpChange(
            int currencyId,
            BigDecimal changePercents,
            BigDecimal valueInRubles,
            BigDecimal previousValue

    ) {}
    public record CurrencyEntity(
            Integer id,
            String code,
            String name,
            BigDecimal valueInRubles
    ) {}
}
