
package com.banking.fraud_detection_service.Model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor

public class FraudCheckresult {
  private boolean fraud;
  private String reason;

}
