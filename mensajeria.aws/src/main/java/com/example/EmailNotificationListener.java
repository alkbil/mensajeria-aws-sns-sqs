   package com.example;

   import io.awspring.cloud.sqs.annotation.SqsListener;
   import io.awspring.cloud.sqs.annotation.SqsListenerAcknowledgementMode;
   import io.awspring.cloud.sqs.listener.acknowledgement.Acknowledgement;
   import org.springframework.stereotype.Component;

   @Component
   public class EmailNotificationListener {

       @SqsListener(
           value = "notificaciones-email",
           acknowledgementMode = SqsListenerAcknowledgementMode.MANUAL
       )
       public void recibir(String mensaje, Acknowledgement acknowledgement) {
           System.out.println("[EMAIL] Procesando: " + mensaje);
           System.out.println("[EMAIL] Enviado correctamente: " + mensaje);
            acknowledgement.acknowledgeAsync();
           
       }
   }