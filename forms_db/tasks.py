import logging
from celery import shared_task
from django.core.management import call_command
from datetime import datetime

logger = logging.getLogger(__name__)

@shared_task(bind=True)
def update_test_logs_task(self):
    """
    Tarea Celery para actualizar los logs de prueba cada 5 minutos
    """
    logger.info(f"Iniciando actualización de logs a las {datetime.now()}")
    
    try:
        call_command('update_test_logs')
        logger.info("Actualización de logs completada exitosamente")
        return {
            'status': 'success',
            'time': str(datetime.now())
        }
    except Exception as e:
        logger.error(f"Error en actualización de logs: {str(e)}", exc_info=True)
        # Reintentar después de 5 minutos si falla
        self.retry(exc=e, countdown=300, max_retries=3)
        return {
            'status': 'error',
            'error': str(e),
            'time': str(datetime.now())
        }
    

@shared_task(bind=True)
def update_1G_logs_task(self):
    logger.info(f"Iniciando actualización 1G de logs a las {datetime.now()}")
    try:
        call_command('1G_update_test_logs')
        logger.info("Actualización 1G de logs completada exitosamente")
        return {'status': 'success', 
                'time': str(datetime.now())
        }
    except Exception as e:
        logger.error(f"Error en actualización 1G de logs: {str(e)}", exc_info=True)
        # Reintentar después de 6 minutos si falla
        self.retry(exc=e, countdown=360, max_retries=3)
        return {
            'status': 'error', 
            'error': str(e), 
            'time': str(datetime.now())
        }