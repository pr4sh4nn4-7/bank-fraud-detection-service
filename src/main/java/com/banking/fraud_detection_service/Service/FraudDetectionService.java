package com.banking.fraud_detection_service.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.fraud_detection_service.Client.AccountServiceClient;
import com.banking.fraud_detection_service.Model.FraudCheckresult;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class FraudDetectionService {

  private final static String VERIFICATION_REQUIRED_TOPIC = "verification.required";
  private final static String FRAUD_CHECK_CLEAN_RESULT_TOPIC = "fraud.check.clean";
  private final AccountServiceClient accountServiceClient;
  private final KafkaTemplate<String, Object> kafkaTemplate;
  private final RedisTemplate<String, String> redisTemplate;
  @Value("${fraud.max-transaction-per-minute}")
  private int maxTransactionPerMinute;
  @Value("${fraud.max-balance-percentage}")
  private double maxBalancePercentage;

  @Value("${fraud.suspicious-amount-multiplier}")
  private double suspiciousAmountMultiplier;

  public void checkTransaction(Map<String, Object> payload) {
    String transactionId = payload.get("transactionId").toString();
    String accountNumber = payload.get("senderAccountNumber").toString();
    BigDecimal amount = new BigDecimal(payload.get("amount").toString());
    BigDecimal senderBalance = accountServiceClient.getBalance(accountNumber);

    // fetch real balance from account service
    log.info("Checking transaction : {} account :{} amount : {} balance: {}", transactionId, accountNumber, amount,
        senderBalance);

    /* perform fraud checks */
    FraudCheckresult result = perfomFraudChecks(accountNumber, amount, senderBalance);
    if (result.isFraud()) {
      log.info("suspicious activity detected - account : {}" + "Reason: {} - requesting otp verification",
          accountNumber, result.getReason());
      Map<String, Object> verificationevent = new HashMap<>();
      verificationevent.put("transactionId", transactionId);
      verificationevent.put("accountNumber", accountNumber);
      verificationevent.put("amount", amount);
      verificationevent.put("reason", result.getReason());

      kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, verificationevent);
    } else {
      // Transaction is clean
      log.info("Transaction clean");
      Map<String, Object> transactionCleanEvent = new HashMap<>();
      transactionCleanEvent.put("transactionId", transactionId);
      transactionCleanEvent.put("isFraud", false);
      transactionCleanEvent.put("reason", null);
      kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC, transactionId, transactionCleanEvent);
    }
  }

  private FraudCheckresult perfomFraudChecks(String accountNumber, BigDecimal amount,
      BigDecimal senderBalance) {

    /*
     * 1. velocity check (checking how much transaction is done in a second. if not
     * usual like more than 5 req in a 1 min )
     *
     * 2. finding average transaction amount , if unual amount than average then
     * mark as fraud
     *
     * 3.
     */

    // pattern 1

    if (isVelocityExceeded(accountNumber)) {
      return new FraudCheckresult(true, "Too many transaction is 60 seconds - velocity limit exceeded");
    }
    // pattern 2 - amount check
    if (isAmountSuspicious(accountNumber, amount)) {

      return new FraudCheckresult(true, "Unusual transaction amount exceeds 3x your average");

    }
    // pattern 3
    if (senderBalance.compareTo(BigDecimal.ZERO) > 0 && isBalanceCheckFailed(senderBalance, amount)) {
      return new FraudCheckresult(true, "Transaction exceeeds 90% of account balance");

    }
    return new FraudCheckresult(false, null);

  }

  private boolean isVelocityExceeded(String accountnUmber) {
    String key = "fraud:velocity" + accountnUmber;
    Long count = redisTemplate.opsForValue().increment(key);

    if (count != null && count == 1) {
      redisTemplate.expire(key, 60, TimeUnit.SECONDS);
    }
    log.info("velocity check amount: {} count : {}/{}", accountnUmber, count, maxTransactionPerMinute);

    return count != null && count > maxTransactionPerMinute;
  }

  private boolean isAmountSuspicious(String accountNumber, BigDecimal amount) {
    String avgKey = "fraud:avg_amount" + accountNumber;
    String avgStr = redisTemplate.opsForValue().get(avgKey);
    if (avgKey == null) {
      redisTemplate.opsForValue().set(avgKey, amount.toString());
    }
    BigDecimal avgAmount = new BigDecimal(avgStr);
    BigDecimal threshold = avgAmount.multiply(BigDecimal.valueOf(suspiciousAmountMultiplier));

    BigDecimal newAvg = avgAmount.add(amount).divide(BigDecimal.valueOf(2), RoundingMode.HALF_UP);

    log.info("Amount check -amount : {} threshold: {} suspicious: {}", amount, threshold,
        amount.compareTo(threshold) > 0);
    return amount.compareTo(threshold) > 0;
  }

  private boolean isBalanceCheckFailed(BigDecimal senderBalance, BigDecimal amount) {
    BigDecimal maxAllowed = senderBalance.multiply(BigDecimal.valueOf(maxBalancePercentage));
    log.info("Balance check - amount {} maxAllowed: {} suspicious: {}", amount, maxAllowed,
        amount.compareTo(maxAllowed) > 0);
    return amount.compareTo(maxAllowed) > 0;

  }

}
