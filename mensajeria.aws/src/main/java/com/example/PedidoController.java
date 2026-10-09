package com.example;

import io.awspring.cloud.sns.core.SnsTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@RestController
@RequestMapping("/api/pedidos")
public class PedidoController {

    private static final String TOPIC_ARN =
        "arn:aws:sns:us-east-1:998877138302:pedido-creado-topic";

    @Autowired
    private SnsTemplate snsTemplate;

    @PostMapping
    public String crearPedido(@RequestParam String cliente) {
        String mensaje = String.format(
            "Pedido de %s creado a las %s",
            cliente,
            LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"))
        );
        snsTemplate.sendNotification(TOPIC_ARN, mensaje, "PedidoCreado");
        return "Pedido publicado: " + mensaje;
    }
}