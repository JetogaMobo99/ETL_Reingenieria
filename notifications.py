import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.utils import formataddr
from email.header import Header
from jinja2 import Template
from dotenv import load_dotenv, find_dotenv
import os
import requests
import json
import asyncio
import aiohttp
from prefect import get_run_logger
from prefect.server.schemas.states import StateType
from prefect.events import emit_event
from prefect.context import get_run_context
from prefect.exceptions import PrefectException
from typing import Optional, Dict, Any
from datetime import datetime

load_dotenv(find_dotenv())

PREFECT_API_URL = os.getenv('PREFECT_API_URL').replace("/api","/runs")


# Configuración de email
SERVER_HOST_MAIL = os.getenv('SERVER_HOST_MAIL')
SERVER_PORT_MAIL = int(os.getenv('SERVER_PORT_MAIL', 587))
SERVER_USER_MAIL = os.getenv('SERVER_USER_MAIL')
SERVER_PASS_MAIL = os.getenv('SERVER_PASS_MAIL')
SERVER_FROM_MAIL = os.getenv('SERVER_FROM_MAIL')

# Configuración de WhatsApp
WHATSAPP_API_URL = 'https://graph.facebook.com/v22.0/678567912009710/messages'
WHATSAPP_ACCESS_TOKEN = 'EAAYOG7qZBOsEBPBDW5rkbZA5qHX1XAXzQ3ZCJqiHLxvRwoNTM56ZAUPLe4xs7y85FTZBicFi962RlMrm5s895TFhDTtEYAxjQ9eZAcw5hMznQrX6T4nMDFs1gTErCnbkREX0tWqmifnN6lQZA3aZCs4885NyooGwrmYJZCQHjUeZCX2yPkoa7EnOI1bR01FodjXdBZBBHXEmkIrp57M4kyXKOPmzafs7AsHaGfNkZBZAeaCXZCsNQVnwZDZD'
WHATSAPP_PHONE_NUMBER = '522201104558'

print("Configuración de correo electrónico cargada correctamente.")
print(f"Servidor: {SERVER_HOST_MAIL}, Puerto: {SERVER_PORT_MAIL}, Usuario: {SERVER_USER_MAIL}, De: {SERVER_FROM_MAIL}")


def render_html_template(template_name: str, **kwargs) -> str:
    """
    Renderiza una plantilla HTML desde la carpeta templates
    
    Args:
        template_name: Nombre del archivo de plantilla (ej: 'flow_failure.html')
        **kwargs: Variables para pasar al template
    
    Returns:
        str: HTML renderizado
    """
    try:
        # Obtener el directorio actual del script
        current_dir = os.path.dirname(os.path.abspath(__file__))
        templates_dir = os.path.join(current_dir, 'templates')
        
        # Verificar que el directorio templates existe
        if not os.path.exists(templates_dir):
            print(f"Templates directory not found: {templates_dir}")
            return f"<p>Error: Templates directory not found</p>"
        
        # Cargar la plantilla
        template_path = os.path.join(templates_dir, template_name)
        if not os.path.exists(template_path):
            print(f"Template file not found: {template_path}")
            return f"<p>Error: Template {template_name} not found</p>"
        
        with open(template_path, 'r', encoding='utf-8') as file:
            template_content = file.read()
        
        # Crear template de Jinja2 y renderizar
        template = Template(template_content)
        rendered_html = template.render(**kwargs)
        
        return rendered_html
        
    except Exception as e:
        print(f"Error rendering template {template_name}: {str(e)}")
        return f"<p>Error rendering template: {str(e)}</p>"


def send_email_notification(subject: str, body: str, to_email = "jetorres@mobo.com.mx"):
    """
    Envía notificación por email
    """


    try:
        mailServer = smtplib.SMTP(SERVER_HOST_MAIL, SERVER_PORT_MAIL)
        mailServer.starttls()
        mailServer.ehlo()
        mailServer.login(user=SERVER_USER_MAIL, password=SERVER_PASS_MAIL)
        
        

        mensaje = MIMEMultipart()
        mensaje.attach(MIMEText(body, "html"))
        mensaje['From'] = formataddr((str(Header("Data Engineering Mobo Reporting Services", 'utf-8')), SERVER_FROM_MAIL))
        mensaje['To'] = to_email 
        mensaje['Subject'] = subject
        
        mailServer.sendmail(mensaje['From'], mensaje['To'].split(","), mensaje.as_string())
        print(f"Email sent successfully")
        mailServer.quit()
        return True
    except Exception as e:
        print(f"Error sending email: {str(e)}")
        return False

def get_text_message_input(recipient: str, text: str) -> str:
    """
    Función auxiliar para formatear el payload del mensaje de WhatsApp
    """
    return json.dumps({
        "messaging_product": "whatsapp",
        "preview_url": False,
        "recipient_type": "individual",
        "to": recipient,
        "type": "text",
        "text": {
            "body": text
        }
    })

async def send_whatsapp_notification_async(message_text: str) -> bool:
    """
    Envía notificación de WhatsApp usando Facebook Graph API con aiohttp (versión async)
    """
    if not WHATSAPP_ACCESS_TOKEN:
        print("WhatsApp access token not configured")
        return False
    
    # Preparar datos del mensaje
    data = get_text_message_input(WHATSAPP_PHONE_NUMBER, message_text)
    
    headers = {
        "Content-type": "application/json",
        "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
    }
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(WHATSAPP_API_URL, data=data, headers=headers) as response:
                if response.status == 200:
                    print("WhatsApp notification sent successfully")
                    return True
                else:
                    print("Status:", response.status)
                    print("Response:", await response.text())
                    return False
        except Exception as e:
            print(f"Error sending WhatsApp notification: {str(e)}")
            return False

def send_whatsapp_notification(message_text: str) -> bool:
    """
    Wrapper sincrónico para la función async de notificación de WhatsApp
    """
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(send_whatsapp_notification_async(message_text))
        loop.close()
        return result
    except Exception as e:
        print(f"Error in sync wrapper: {str(e)}")
        return False

def notify_failure(flow, flow_run, state):
    """
    Función para notificar fallas de flows
    Uso: @my_flow.on_failure
    """
    logger = get_run_logger()
    logger.info("Flow failure detected. Sending notifications...")
    
    execution_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Preparar email
    subject = f"❌ Error en Flow: {flow_run.name}"
    email_body = render_html_template(
        'email_notification.html', 
        flow_run=flow_run,
        state=state,
        PREFECT_API_URL=PREFECT_API_URL,
        macros={'datetime': datetime}
    )
    
    # Preparar WhatsApp
    whatsapp_message = f"Your job {flow_run.name} entered {state.name} "\
            f"with message:\n\n"\
            f"See {PREFECT_API_URL}/flow-run/" \
            f"flow-run/{flow_run.id}   |the flow run in the UI   \n\n" \
            f"Tags: {flow_run.tags}\n\n"\
            f"Scheduled start: {flow_run.expected_start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"\
            f"Total execution time: {int(flow_run.total_run_time.total_seconds()) // 60:02d}:{int(flow_run.total_run_time.total_seconds()) % 60:02d}"

    # Enviar notificaciones
    email_success = send_email_notification(subject, email_body)
    whatsapp_success = send_whatsapp_notification(whatsapp_message)
    
    if email_success and whatsapp_success:
        logger.info("All failure notifications sent successfully.")
    elif email_success:
        logger.warning("Email sent successfully, but WhatsApp notification failed.")
    elif whatsapp_success:
        logger.warning("WhatsApp sent successfully, but email notification failed.")
    else:
        logger.error("Both email and WhatsApp notifications failed.")

def notify_completion(flow, flow_run, state):
    """
    Función para notificar completación exitosa de flows
    Uso: @my_flow.on_completion
    """
    logger = get_run_logger()
    logger.info("Flow completion detected. Sending notifications...")
    
    execution_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')




    # Preparar email
    subject = f"✅ Flow Completado: {flow_run.name}"
    email_body = render_html_template(
        'email_notification.html', 
        flow_run=flow_run,
        state=state,
        PREFECT_API_URL=PREFECT_API_URL,
        macros={'datetime': datetime}
    )

    # Preparar WhatsApp
    whatsapp_message = f"🎉 *¡Flow Completado Exitosamente!* 🎉\n\n"\
            f"📋 *Nombre del Job:* {flow_run.name}\n"\
            f"✅ *Estado:* {state.name}\n\n"\
            f"🔗 *Ver en UI:* {PREFECT_API_URL}/flow-run/{flow_run.id}\n\n"\
            f"🏷️ *Tags:* {flow_run.tags}\n"\
            f"⏰ *Inicio programado:* {flow_run.expected_start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"\
            f"⌛ *Tiempo total:* {int(flow_run.total_run_time.total_seconds()) // 60:02d}:{int(flow_run.total_run_time.total_seconds()) % 60:02d} min\n\n"\
            f"💚 *¡Proceso ejecutado correctamente!*"
    
    # Enviar notificaciones
    email_success = send_email_notification(subject, email_body)
    whatsapp_success = send_whatsapp_notification(whatsapp_message)
    
    if email_success and whatsapp_success:
        logger.info("All completion notifications sent successfully.")
    elif email_success:
        logger.warning("Email sent successfully, but WhatsApp notification failed.")
    elif whatsapp_success:
        logger.warning("WhatsApp sent successfully, but email notification failed.")
    else:
        logger.error("Both email and WhatsApp notifications failed.")
