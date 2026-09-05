package com.aegis.surveillance.model;

import java.math.BigDecimal;
import java.time.Instant;

public record TradeEvent(
    String tradeId,
    String orderId,
    Instant eventTime,
    String instrument,
    String side,
    BigDecimal quantity,
    BigDecimal price,
    String accountId,
    String clientId,
    String venue) {}
