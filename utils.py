
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from email.header import Header
from dotenv import load_dotenv, find_dotenv
import os
import json
import asyncio
import aiohttp
from datetime import datetime
from typing import Optional, Dict, Any
from prefect import flow, task, get_run_logger
from prefect.context import get_run_context

load_dotenv(find_dotenv())

# Configuración de email
SERVER_HOST_MAIL = os.getenv('SERVER_HOST_MAIL')
SERVER_PORT_MAIL = int(os.getenv('SERVER_PORT_MAIL', 587))
SERVER_USER_MAIL = os.getenv('SERVER_USER_MAIL')
SERVER_PASS_MAIL = os.getenv('SERVER_PASS_MAIL')
SERVER_FROM_MAIL = os.getenv('SERVER_FROM_MAIL')

# Configuración de WhatsApp
WHATSAPP_API_URL = 'https://graph.facebook.com/v22.0/678567912009710/messages'
WHATSAPP_ACCESS_TOKEN = os.getenv('WHATSAPP_ACCESS_TOKEN', '')
WHATSAPP_PHONE_NUMBER = os.getenv('WHATSAPP_PHONE_NUMBER', '522201104558')

def send_flow_notification(success: bool = True, flow_name: str = None, error_message: str = None, 
                          execution_time: str = None, custom_message: str = None, 
                          send_email: bool = True, send_whatsapp: bool = True):
    """
    Función principal para enviar notificaciones de éxito o fallo en flows de Prefect
    
    Args:
        success (bool): True si el flow fue exitoso, False si falló
        flow_name (str): Nombre del flow (se auto-detecta si no se proporciona)
        error_message (str): Mensaje de error (solo para fallos)
        execution_time (str): Tiempo de ejecución (opcional)
        custom_message (str): Mensaje personalizado (opcional)
        send_email (bool): Si enviar notificación por email
        send_whatsapp (bool): Si enviar notificación por WhatsApp
    """
    
    # Auto-detectar información del flow si está disponible
    try:
        run_context = get_run_context()
        flow_run = run_context.flow_run
        
        if not flow_name:
            flow_name = flow_run.flow_name
        
        flow_run_id = str(flow_run.id)
        start_time = flow_run.start_time
        current_time = datetime.now()
        
        if execution_time is None and start_time:
            duration = current_time - start_time
            execution_time = str(duration).split('.')[0]  # Remover microsegundos
            
    except:
        # Si no hay contexto de Prefect disponible, usar valores por defecto
        flow_name = flow_name or "Unknown Flow"
        flow_run_id = "manual-execution"
        start_time = datetime.now()
        current_time = datetime.now()
        execution_time = execution_time or "N/A"
    
    # Crear contexto para las notificaciones
    notification_context = {
        'flow_name': flow_name,
        'flow_run_id': flow_run_id,
        'success': success,
        'start_time': start_time,
        'end_time': current_time,
        'execution_time': execution_time,
        'error_message': error_message,
        'custom_message': custom_message
    }
    
    # Enviar notificaciones según se solicite
    results = {}
    
    if send_email:
        results['email'] = _send_email_notification(notification_context)
    
    if send_whatsapp:
        results['whatsapp'] = _send_whatsapp_notification(notification_context)
    
    # Log de resultados
    status = "SUCCESS" if success else "FAILURE"
    print(f"📧 Notificación {status} enviada para flow: {flow_name}")
    
    if send_email:
        print(f"   Email: {'✅ Enviado' if results.get('email') else '❌ Falló'}")
    if send_whatsapp:
        print(f"   WhatsApp: {'✅ Enviado' if results.get('whatsapp') else '❌ Falló'}")
    
    return results

def _send_email_notification(context: Dict[str, Any]) -> bool:
    """Envía notificación por email"""
    try:
        success = context['success']
        flow_name = context['flow_name']
        execution_time = context['execution_time']
        error_message = context.get('error_message', '')
        custom_message = context.get('custom_message', '')
        
        # Configurar asunto según el resultado
        if success:
            subject = f"✅ Flow Exitoso: {flow_name}"
            status_emoji = "✅"
            status_text = "COMPLETADO EXITOSAMENTE"
            status_color = "#28a745"
        else:
            subject = f"❌ Flow Falló: {flow_name}"
            status_emoji = "❌"
            status_text = "FALLÓ"
            status_color = "#dc3545"
        
        # Crear cuerpo del email
        body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: {status_color}; color: white; padding: 15px; border-radius: 5px; }}
                .content {{ padding: 20px; border: 1px solid #ddd; border-radius: 5px; margin-top: 10px; }}
                .info {{ margin: 10px 0; }}
                .error {{ background-color: #f8d7da; padding: 10px; border-radius: 5px; color: #721c24; }}
                .success {{ background-color: #d4edda; padding: 10px; border-radius: 5px; color: #155724; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>{status_emoji} Notificación de Prefect Flow</h2>
            </div>
            <div class="content">
                <div class="info"><strong>Flow:</strong> {flow_name}</div>
                <div class="info"><strong>Estado:</strong> {status_text}</div>
                <div class="info"><strong>Tiempo de Ejecución:</strong> {execution_time}</div>
                <div class="info"><strong>Fecha/Hora:</strong> {context['end_time'].strftime('%Y-%m-%d %H:%M:%S')}</div>
                <div class="info"><strong>Run ID:</strong> {context['flow_run_id']}</div>
        """
        
        if custom_message:
            body += f'<div class="info"><strong>Mensaje:</strong> {custom_message}</div>'
        
        if not success and error_message:
            body += f'<div class="error"><strong>Error:</strong> {error_message}</div>'
        
        if success:
            body += '<div class="success">El flow se ejecutó correctamente sin errores.</div>'
        
        body += """
            </div>
            <br>
            <p><em>Este es un mensaje automático del sistema de monitoreo de Prefect.</em></p>
        </body>
        </html>
        """
        
        # Enviar email
        mailServer = smtplib.SMTP(SERVER_HOST_MAIL, SERVER_PORT_MAIL)
        mailServer.starttls()
        mailServer.ehlo()
        mailServer.login(user=SERVER_USER_MAIL, password=SERVER_PASS_MAIL)
        
        mensaje = MIMEMultipart()
        mensaje.attach(MIMEText(body, "html"))
        mensaje['From'] = formataddr((str(Header("Data Engineering Mobo Reporting Services", 'utf-8')), SERVER_FROM_MAIL))
        mensaje['To'] = 'jetorres@mobo.com.mx'
        mensaje['Subject'] = subject
        
        mailServer.sendmail(mensaje['From'], mensaje['To'], mensaje.as_string())
        mailServer.quit()
        
        return True
        
    except Exception as e:
        print(f"Error enviando email: {str(e)}")
        return False

def _send_whatsapp_notification(context: Dict[str, Any]) -> bool:
    """Envía notificación por WhatsApp"""
    if not WHATSAPP_ACCESS_TOKEN:
        print("WhatsApp token no configurado")
        return False
    
    try:
        success = context['success']
        flow_name = context['flow_name']
        execution_time = context['execution_time']
        error_message = context.get('error_message', '')
        custom_message = context.get('custom_message', '')
        
        # Crear mensaje según el resultado
        if success:
            message = f"✅ *Flow Completado Exitosamente* ✅\n\n"
            message += f"📊 *Flow:* {flow_name}\n"
            message += f"⏱️ *Tiempo:* {execution_time}\n"
            message += f"📅 *Fecha:* {context['end_time'].strftime('%Y-%m-%d %H:%M:%S')}\n"
            message += f"🎉 *Estado:* COMPLETADO\n"
            
            if custom_message:
                message += f"\n💬 *Mensaje:* {custom_message}"
        else:
            message = f"🚨 *Flow Falló* 🚨\n\n"
            message += f"📊 *Flow:* {flow_name}\n"
            message += f"⏱️ *Tiempo:* {execution_time}\n"
            message += f"📅 *Fecha:* {context['end_time'].strftime('%Y-%m-%d %H:%M:%S')}\n"
            message += f"❌ *Estado:* FALLÓ\n"
            
            if error_message:
                message += f"\n🔍 *Error:* {error_message}"
            
            if custom_message:
                message += f"\n💬 *Mensaje:* {custom_message}"
            
            message += f"\n\nPor favor revisa los logs de Prefect para más detalles."
        
        # Enviar WhatsApp
        data = json.dumps({
            "messaging_product": "whatsapp",
            "preview_url": False,
            "recipient_type": "individual",
            "to": WHATSAPP_PHONE_NUMBER,
            "type": "text",
            "text": {"body": message}
        })
        
        headers = {
            "Content-type": "application/json",
            "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
        }
        
        # Usar asyncio para envío asíncrono
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(_send_whatsapp_async(data, headers))
        loop.close()
        
        return result
        
    except Exception as e:
        print(f"Error enviando WhatsApp: {str(e)}")
        return False

async def _send_whatsapp_async(data, headers):
    """Función asíncrona para enviar WhatsApp"""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(WHATSAPP_API_URL, data=data, headers=headers) as response:
                return response.status == 200
        except Exception:
            return False

# ============================================================================
# FUNCIONES DE CONVENIENCIA
# ============================================================================

def notify_success(flow_name: str = None, execution_time: str = None, message: str = None):
    """
    Notifica que un flow se ejecutó exitosamente
    
    Args:
        flow_name: Nombre del flow (opcional, se auto-detecta)
        execution_time: Tiempo de ejecución (opcional)
        message: Mensaje personalizado (opcional)
    """
    return send_flow_notification(
        success=True,
        flow_name=flow_name,
        execution_time=execution_time,
        custom_message=message
    )

def notify_failure(flow_name: str = None, error: str = None, execution_time: str = None, message: str = None):
    """
    Notifica que un flow falló
    
    Args:
        flow_name: Nombre del flow (opcional, se auto-detecta)
        error: Mensaje de error
        execution_time: Tiempo de ejecución (opcional)
        message: Mensaje personalizado adicional (opcional)
    """
    return send_flow_notification(
        success=False,
        flow_name=flow_name,
        error_message=error,
        execution_time=execution_time,
        custom_message=message
    )
