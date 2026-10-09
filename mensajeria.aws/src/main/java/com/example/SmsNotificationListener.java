package com.example;

import io.awspring.cloud.sqs.annotation.SqsListener;
import org.springframework.stereotype.Component;

@Component
public class SmsNotificationListener {

    @SqsListener("notificaciones-sms")
    public void recibir(String mensaje) {
        System.out.println("[SMS] Enviando SMS con: " + mensaje);
    }
}