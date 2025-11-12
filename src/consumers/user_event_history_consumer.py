"""
Kafka Historical User Event Consumer (Event Sourcing)
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import json
from logger import Logger
from typing import Optional
import os
from kafka import KafkaConsumer
from pathlib import Path
from handlers.handler_registry import HandlerRegistry

class UserEventHistoryConsumer:
    """A consumer that starts reading Kafka events from the earliest point from a given topic"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        registry: HandlerRegistry
    ):
        # TODO: définir les paramètres corrects
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id + "_history"
        self.registry = registry
        self.auto_offset_reset = "earliest" 
        self.consumer: Optional[KafkaConsumer] = None
        self.logger = Logger.get_instance("UserEventHistoryConsumer")
    
    def start(self) -> None:
        """Start consuming messages from Kafka"""
        self.logger.info(f"Démarrer un consommateur : {self.group_id}")

        
        
        try:
            # TODO: implémentation basée sur UserEventConsumer
            # TODO: enregistrez les événements dans un fichier JSON
            self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),  
            enable_auto_commit=True,
            consumer_timeout_ms=5000 
            )
        
            events = []
            for message in self.consumer:
                events.append(message.value)
                self.logger.debug(f"Evèmnement lu : {message.value}")
            
            output_dir = "/app/output"
            os.makedirs(output_dir, exist_ok=True)
            filename = os.path.join(output_dir, "user_event_history.json")

            with open(filename, 'w', encoding='utf-8') as json_file:
                json.dump(events, json_file, ensure_ascii=False, indent=4)    
                json_file.write("\n")   

            self.logger.debug(f"Historique des événements enregistré dans {filename}")    

        except Exception as e:
            self.logger.error(f"Erreur: {e}", exc_info=True)
        finally:
            self.stop()


    def stop(self) -> None:
        """Stop the consumer gracefully"""
        if self.consumer:
            self.consumer.close()
            self.logger.info("Arrêter le consommateur!")