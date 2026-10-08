package kafka.integration.consumers;

import kafka.integration.models.Currency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static kafka.integration.db.DBManager.*;

public class CurrencyConsumer {
    private static final Logger log = LoggerFactory.getLogger(CurrencyConsumer.class);

    public static void processMsg(Map<String, Object> rawCurrencyMap) {
        List<Currency.CurrencyStructured> structuredCurrencies = new ArrayList<>();
        rawCurrencyMap.forEach((code, v) -> {
            if (!(v instanceof Map<?, ?> parsed)) {
                return;
            }
            Map<String, Object> data = (Map<String, Object>) parsed;

            Number valueNum = (Number) data.getOrDefault("Value", 0);
            BigDecimal value = BigDecimal.valueOf(valueNum.doubleValue());
            Number nominal = (Number) data.getOrDefault("Nominal", 1);

            BigDecimal valueInRub = value.divide(BigDecimal.valueOf(nominal.longValue()), 4, RoundingMode.HALF_EVEN);
            String name = (String) data.getOrDefault("Name", code);

            Currency.CurrencyStructured structured = new Currency.CurrencyStructured(code, name, valueInRub);
            structuredCurrencies.add(structured);
        });
        List<Currency.CurrencyEntity> savedCurrency = saveCurrencies(structuredCurrencies);
        if (savedCurrency == null || savedCurrency.isEmpty()) {
            return;
        }
        boolean historySaved = saveCurrenciesHistory(structuredCurrencies);
        if (!historySaved) {
            return;
        }
        var sharpChanges = checkSharpChanges(savedCurrency);
        if (!sharpChanges.isEmpty()) {
            saveCurrencySharpChanges(sharpChanges);
        }
        log.info("Currency batch successfully processed");
    }

    private static List<Currency.SharpChange> checkSharpChanges(List<Currency.CurrencyEntity> data) {
        List<Currency.SharpChange> sharpChanges = new ArrayList<>();
        for (var currency : data) {
            Currency.SharpChange sharpChange = compareRates(currency);
            if (sharpChange != null) {
                sharpChanges.add(sharpChange);
            }
        }
        return sharpChanges;
    }
    private static Currency.SharpChange compareRates(Currency.CurrencyEntity currency) {
        int radiusHours = 24;
        String code = currency.code();
        BigDecimal valueRub = currency.valueInRubles();

        Instant since = Instant.now().minus(Duration.ofHours(radiusHours));

        List<Currency.CurrencyStructured> historyRates = getCurrencyHistory(code, since);
        List<BigDecimal> rates = new ArrayList<>();

        for (var hr : historyRates) {
            rates.add(hr.valueInRubles());
        }
        if (rates.isEmpty()) {
            return null;
        }
        BigDecimal avgRate = rates.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal threshold = BigDecimal.valueOf(0.03);
        boolean isSharpChange = (valueRub.subtract(avgRate).abs()).compareTo(threshold.multiply(avgRate)) > 0;
        if (!isSharpChange) {
            return null;
        }
        log.info("Currency sharp change detected in valute {}", code);
        BigDecimal changePercents = valueRub.subtract(avgRate).divide(avgRate, 4, RoundingMode.HALF_EVEN).multiply(BigDecimal.valueOf(100));
        return new Currency.SharpChange(
                currency.id(),
                changePercents,
                valueRub,
                avgRate
        );
    }
}
