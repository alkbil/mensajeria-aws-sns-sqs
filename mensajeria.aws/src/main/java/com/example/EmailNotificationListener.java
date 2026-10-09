package com.example;

import io.awspring.cloud.sqs.annotation.SqsListener;
import org.springframework.stereotype.Component;

@Component
public class EmailNotificationListener {

    @SqsListener("notificaciones-email")
    public void recibir(String mensaje) {
        System.out.println("[EMAIL] Enviando correo con: " + mensaje);
    }
}