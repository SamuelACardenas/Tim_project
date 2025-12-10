import os
import paramiko
import re
import json
import base64
from pathlib import Path
from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import datetime, timedelta
from django.conf import settings
from django.contrib.auth.models import User
from forms_db.models import Uut, TestHistory, Station, Employes, Booms, Failures, ErrorMessages
from io import StringIO
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from suds.client import Client
import socket

class Command(BaseCommand):
    help = 'Actualiza logs de prueba desde estaciones remotas para proyecto TIM'
    
    def handle(self, *args, **options):
        """
        ============================================================================
        MODIFICACIÓN COMIENZO: Flujo simplificado - Sin carga inicial de JSON
        Ahora extraemos las sesiones directamente desde cada estación RUNIN
        ============================================================================
        """
        
        estaciones_dict = [
            #{"ip": "10.12.199.150", "nombre": "RUNIN 01", "usuario": "User"},
            #{"ip": "10.12.199.155", "nombre": "RUNIN 02", "usuario": "user"},
            #{"ip": "10.12.199.160", "nombre": "RUNIN 03", "usuario": "User"},
            {"ip": "10.12.199.165", "nombre": "RUNIN 04", "usuario": "user"},
            #{"ip": "10.12.199.140", "nombre": "BFT 01", "usuario": "User.DESKTOP-T3G3HJ2"},
            #{"ip": "10.12.199.145", "nombre": "BFT 02", "usuario": "User"},
            #{"ip": "10.12.199.170", "nombre": "FCA 01", "usuario": "user"},
            #{"ip": "10.12.199.175", "nombre": "FCA 02", "usuario": "user"},
        ]
        
        for estacion in estaciones_dict:
            ip = estacion["ip"]
            nombre = estacion["nombre"]
            usuario_default = estacion["usuario"]
            
            # Inicialmente no tenemos sesión, se determinará al conectar
            session_employee = None
            
            # Usar credenciales por defecto
            password = self.get_password(nombre)
            
            self.stdout.write(self.style.SUCCESS(
                f"Procesando estación: {nombre} ({ip})"
            ))
            
            try:
                self.process_station(ip, nombre, usuario_default, password, session_employee)
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Error en estación {nombre}: {str(e)}'))
        """
        ============================================================================
        MODIFICACIÓN FIN: Flujo simplificado
        ============================================================================
        """

    """
    ============================================================================
    NUEVOS MÉTODOS COMIENZO: Extracción de sesiones JSON desde estaciones
    ============================================================================
    """
    
    def extract_session_from_station(self, sftp, ip, estacion_nombre):
        """
        Extrae el archivo de sesiones JSON directamente desde la estación Windows
        Busca en múltiples ubicaciones posibles donde podría estar el archivo
        """
        try:
            self.stdout.write(f"Buscando archivo de sesiones en {estacion_nombre}...")
            
            # Rutas posibles donde podría estar el archivo JSON
            possible_paths = [
                # Escritorio del usuario (lo más común)
                "C:/Users/User/Desktop/station_sessions.json",
                "C:/Users/User/Downloads/station_sessions.json",
                "C:/Users/User/Documents/station_sessions.json",
                
                # Directorio de la aplicación (si se ejecuta desde ahí)
                "C:/TIM/station_sessions.json",
                
                # Directorio raíz o temporal
                "C:/station_sessions.json",
                "C:/Windows/Temp/station_sessions.json",
            ]
            
            json_content = None
            
            for remote_path in possible_paths:
                try:
                    self.stdout.write(f"  Probando: {remote_path}")
                    sftp.stat(remote_path)  # Verifica si existe
                    
                    # Leer el archivo
                    with sftp.open(remote_path, 'rb') as remote_file:
                        file_content = remote_file.read()
                        json_content = json.loads(file_content.decode('utf-8'))
                    
                    self.stdout.write(self.style.SUCCESS(f"✓ Archivo encontrado en: {remote_path}"))
                    
                    # Buscar sesión específica para esta estación
                    if estacion_nombre in json_content:
                        station_session = json_content[estacion_nombre]
                        
                        # Verificar que tenga logged_in = True
                        if station_session.get('logged_in', False):
                            self.stdout.write(self.style.SUCCESS(
                                f"✓ Sesión activa encontrada para {estacion_nombre}"
                            ))
                            return station_session
                        else:
                            self.stdout.write(self.style.WARNING(
                                f"Sesión encontrada pero logged_in=False"
                            ))
                    else:
                        self.stdout.write(self.style.WARNING(
                            f"No hay sesión para {estacion_nombre} en el archivo"
                        ))
                    
                    break  # Salir si encontramos el archivo
                    
                except FileNotFoundError:
                    continue  # Intentar siguiente ruta
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f"Error leyendo {remote_path}: {str(e)}"))
                    continue
            
            if json_content is None:
                self.stdout.write(self.style.WARNING(
                    f"No se encontró archivo de sesiones en {estacion_nombre}"
                ))
            
            return None
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error en extract_session_from_station: {str(e)}"))
            return None

    def get_employee_from_json_session(self, json_session):
        """
        Obtiene el objeto Employes desde el JSON de sesión
        El JSON tiene la estructura de la respuesta API que incluye user_data
        """
        try:
            # El JSON tiene esta estructura:
            # {
            #   'station_ip': '10.12.199.165',
            #   'username': 'gAAAAAB...',  # Encriptado
            #   'password': 'gAAAAAB...',  # Encriptado
            #   'user_data': { ... },      # Respuesta completa de la API
            #   'timestamp': 1234567890,
            #   'logged_in': True
            # }
            
            user_data = json_session.get('user_data', {})
            
            if not user_data.get('success', False):
                self.stdout.write(self.style.WARNING(
                    "user_data no tiene success=True en JSON"
                ))
                return None
            
            # Extraer username del user_data (ya viene desencriptado en la respuesta API)
            user_info = user_data.get('user', {})
            username = user_info.get('username')
            
            if not username:
                self.stdout.write(self.style.WARNING(
                    "No se encontró username en user_data"
                ))
                return None
            
            self.stdout.write(f"Buscando empleado para usuario: {username}")
            
            # Buscar el usuario en la base de datos
            user = User.objects.get(username=username)
            
            # Buscar el empleado asociado
            employee = Employes.objects.get(employeeNumber=user)
            
            self.stdout.write(self.style.SUCCESS(
                f"Empleado encontrado desde JSON: {employee.employeeName} ({username})"
            ))
            
            return employee
            
        except User.DoesNotExist:
            self.stdout.write(self.style.ERROR(
                f"Usuario no encontrado en BD: {username}"
            ))
            return None
        except Employes.DoesNotExist:
            self.stdout.write(self.style.ERROR(
                f"Empleado no encontrado para usuario: {username}"
            ))
            return None
        except Exception as e:
            self.stdout.write(self.style.ERROR(
                f"Error obteniendo empleado desde JSON: {e}"
            ))
            return None
    
    """
    ============================================================================
    NUEVOS MÉTODOS FIN: Extracción de sesiones JSON desde estaciones
    ============================================================================
    """

    def get_password(self, nombre_estacion):
        nombre = nombre_estacion.upper()
        numero = nombre_estacion[-2:].strip().zfill(2)

        if "RUNIN" in nombre:
            return f"Runin{numero}"
        elif "BFT" in nombre:
            return f"Bft{numero}"
        elif "FCA" in nombre:
            return f"Fca{numero}"
        else:
            return "default_password"

    def process_station_with_session(self, ip, estacion_nombre, usuario, password, session_employee):
        """
        Procesa estación RUNIN con credenciales de sesión
        MODIFICADO: Ahora también busca JSON en la estación
        """
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            self.stdout.write(self.style.SUCCESS(
                f"Intentando conectar a {estacion_nombre} ({ip}) con usuario de sesión '{usuario}'"
            ))

            client.connect(ip, port=22, username=usuario, password=password, timeout=10)

            self.stdout.write(self.style.SUCCESS(
                f"Conectado exitosamente a {estacion_nombre} ({ip}) con usuario de sesión"
            ))

            sftp = client.open_sftp()
            
            """
            ============================================================================
            MODIFICACIÓN COMIENZO: Buscar archivo de sesiones en el escritorio de Windows
            ============================================================================
            """
            try:
                json_session = self.extract_session_from_station(sftp, ip, estacion_nombre)
                
                if json_session and 'user_data' in json_session:
                    # Obtener empleado desde el JSON de sesión
                    session_employee = self.get_employee_from_json_session(json_session)
                    if session_employee:
                        self.stdout.write(self.style.SUCCESS(
                            f"Empleado obtenido desde sesión JSON: {session_employee.employeeName}"
                        ))
                    else:
                        self.stdout.write(self.style.WARNING(
                            "No se pudo obtener empleado desde JSON, usando credenciales SSH"
                        ))
                else:
                    self.stdout.write(self.style.WARNING(
                        f"No se encontró sesión JSON válida en {estacion_nombre}"
                    ))
            except Exception as e:
                self.stdout.write(self.style.WARNING(
                    f"Error extrayendo sesión JSON: {str(e)}"
                ))
            """
            ============================================================================
            MODIFICACIÓN FIN: Buscar archivo de sesiones
            ============================================================================
            """

            # Verificar/Crear directorio TIM log remoto
            try:
                sftp.chdir('C:/LOG/TIM')
            except FileNotFoundError:
                sftp.mkdir('C:/LOG/TIM')
                sftp.chdir('C:/LOG/TIM')

            archivos_remotos = sftp.listdir()

            for archivo in archivos_remotos:
                if archivo.endswith((".log")) and ("[FAIL]" in archivo or "[PASS]" in archivo):
                    try:
                        self.process_single_file(sftp, archivo, ip, estacion_nombre, session_employee)
                    except Exception as e:
                        self.stdout.write(self.style.WARNING(f'Error procesando {archivo}: {str(e)}'))
                else:
                    pass

            sftp.close()

        except paramiko.AuthenticationException:
            self.stdout.write(self.style.ERROR(
                f"Error de autenticación en {estacion_nombre} con usuario de sesión {usuario}"
            ))
            # Fallback a credenciales por defecto
            default_password = self.get_password(estacion_nombre)
            self.stdout.write(self.style.WARNING(
                f"Intentando con credenciales por defecto para {estacion_nombre}"
            ))
            self.process_station(ip, estacion_nombre, 'PMDU', default_password, None)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error de conexión con {ip}: {str(e)}'))
        finally:
            client.close()

    def process_station(self, ip, estacion_nombre, usuario, password, session_employee=None):
        """
        Procesa estación con credenciales por defecto
        MODIFICADO: Ahora también busca JSON para estaciones RUNIN
        """
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            self.stdout.write(self.style.SUCCESS(
                f"Intentando conectar a {estacion_nombre} ({ip}) con usuario por defecto '{usuario}'"
            ))

            client.connect(ip, port=22, username=usuario, password=password, timeout=10)

            self.stdout.write(self.style.SUCCESS(
                f"Conectado exitosamente a {estacion_nombre} ({ip})"
            ))

            sftp = client.open_sftp()
            
            """
            ============================================================================
            MODIFICACIÓN COMIENZO: Buscar JSON para estaciones RUNIN con credenciales por defecto
            ============================================================================
            """
            if "RUNIN" in estacion_nombre.upper():
                try:
                    json_session = self.extract_session_from_station(sftp, ip, estacion_nombre)
                    
                    if json_session and 'user_data' in json_session:
                        # Obtener empleado desde el JSON de sesión
                        session_employee = self.get_employee_from_json_session(json_session)
                        if session_employee:
                            self.stdout.write(self.style.SUCCESS(
                                f"Empleado obtenido desde sesión JSON: {session_employee.employeeName}"
                            ))
                except Exception as e:
                    self.stdout.write(self.style.WARNING(
                        f"Error buscando JSON en {estacion_nombre}: {str(e)}"
                    ))
            """
            ============================================================================
            MODIFICACIÓN FIN: Buscar JSON para estaciones RUNIN
            ============================================================================
            """

            # Verificar/Crear directorio TIM log remoto
            try:
                sftp.chdir('C:/LOG/TIM')
            except FileNotFoundError:
                sftp.mkdir('C:/LOG/TIM')
                sftp.chdir('C:/LOG/TIM')

            archivos_remotos = sftp.listdir()

            for archivo in archivos_remotos:
                if archivo.endswith((".log")) and ("[FAIL]" in archivo or "[PASS]" in archivo):
                    try:
                        self.process_single_file(sftp, archivo, ip, estacion_nombre, session_employee)
                    except Exception as e:
                        self.stdout.write(self.style.WARNING(f'Error procesando {archivo}: {str(e)}'))
                else:
                    pass

            sftp.close()

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error de conexión con {ip}: {str(e)}'))
        finally:
            client.close()

    def process_single_file(self, sftp, filename, ip, estacion_nombre, session_employee=None):
        """
        Procesa un archivo individual - MODIFICADO para aceptar session_employee
        """
        remote_path = filename 
        remote_backup_path = f"processed/{filename}"
        is_pass = "PASS" in filename
        
        try:
            """
            ============================================================================
            CAMBIO COMIENZO: Primero guardar copia local SIEMPRE (GDL o NO-GDL)
            ============================================================================
            """
            # 1. Guardar copia local del archivo original (esto siempre se hace)
            log_info = self.save_local_copy(sftp, remote_path, filename, estacion_nombre)
            
            # Verificar si tenemos un SN válido
            if not log_info.get('sn'):
                self.stdout.write(self.style.ERROR(
                    f"No se pudo extraer SN del archivo {filename}. No se procesará."
                ))
                # No movemos el archivo, queda para procesar manualmente
                return
            """
            ============================================================================
            CAMBIO FIN: Primero guardar copia local SIEMPRE
            ============================================================================
            """
            
            """
            ============================================================================
            CAMBIO COMIENZO: Lógica de consulta SOAP con manejo de errores de red
            ============================================================================
            """
            resultado_test = 'PASS' if is_pass else 'FAIL'
            
            try:
                # Verificar si hay conectividad de red antes de intentar SOAP
                if not self.check_network_connectivity():
                    self.stdout.write(self.style.WARNING(
                        f"Sin conectividad de red. No se consultará SOAP para {filename}. "
                        f"El archivo NO será movido y se reintentará después."
                    ))
                    return  # Salir sin mover el archivo
                
                # Llamada a la función de procesamiento de reglas SOAP
                resultado_reglas = self.procesar_test_completo(
                    serial_number=log_info['sn'],
                    resultado_test=resultado_test,
                    test_history_model=TestHistory
                )
                
                factory_value = resultado_reglas['stage_final']
                
            except Exception as soap_error:
                self.stdout.write(self.style.ERROR(
                    f'Error grave en consulta SOAP para {filename}: {str(soap_error)}'
                ))
                self.stdout.write(self.style.WARNING(
                    f'El archivo {filename} NO será movido. Se reintentará en la próxima ejecución.'
                ))
                return  # Salir sin mover el archivo
            """
            ============================================================================
            CAMBIO FIN: Lógica de consulta SOAP con manejo de errores de red
            ============================================================================

            # ============================================================================
            # CAMBIO COMIENZO: Solo ahora mover el archivo después de procesamiento exitoso
            # ============================================================================
            """
            try:
                sftp.mkdir('C:/LOG/TIM/processed')
            except:
                pass  # El directorio ya existe
            
            # Mover el archivo remoto SOLO si todo fue exitoso
            sftp.rename(remote_path, remote_backup_path)
            
            self.stdout.write(self.style.SUCCESS(
                f'Archivo movido a processed: {filename}'
            ))"""
            # ============================================================================
            # CAMBIO FIN: Solo ahora mover el archivo después de procesamiento exitoso
            # ============================================================================

            """
            ============================================================================
            CAMBIO COMIENZO: Registrar en BD solo si es GDL
            ============================================================================
            """
            if factory_value.upper() == 'GDL':
                """
                ============================================================================
                MODIFICACIÓN: Pasar el empleado de sesión a los métodos de registro
                ============================================================================
                """
                uut = self.register_uut(log_info, is_pass, session_employee)
                test_history = self.register_test_history(uut, ip, estacion_nombre, log_info, is_pass, session_employee)
                
                if not is_pass:
                    self.register_failure(uut, ip, estacion_nombre, log_info, session_employee)
                """
                ============================================================================
                MODIFICACIÓN FIN: Pasar el empleado de sesión a los métodos de registro
                ============================================================================
                
                """
                ============================================================================
                MODIFICACIÓN COMIENZO: Mostrar información del empleado en el log
                ============================================================================
                """
                employee_info = session_employee.employeeName if session_employee else "Sistema"
                self.stdout.write(self.style.SUCCESS(
                    f"Procesado (GDL): {filename} | SN: {log_info['sn']} | Empleado: {employee_info} | {'PASS' if is_pass else 'FAIL'}"
                ))
                """
                ============================================================================
                MODIFICACIÓN FIN: Mostrar información del empleado en el log
                ============================================================================
            else:
                # NO-GDL: Solo copia local y mover archivo, no registro en BD
                self.stdout.write(self.style.SUCCESS(
                    f"Procesado (NO-GDL): {filename} | SN: {log_info.get('sn', 'N/A')} | "
                    f"Copia local guardada, archivo movido, NO registro en BD"
                ))
            """
            ============================================================================
            CAMBIO FIN: Registrar en BD solo si es GDL
            ============================================================================
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(
                f'Error procesando archivo {filename}: {str(e)}'
            ))
            # En caso de error, NO mover el archivo para reintentar después
            raise

    def save_local_copy(self, sftp, remote_path, filename, estacion_nombre):
        """
        Guarda copia local exacta del archivo original y devuelve la información parseada
        """
        try:
            # Obtener ruta base según configuración
            base_dir = Path(settings.STATIC_ROOT)
            
            # Leer y parsear el archivo remoto primero
            with sftp.open(remote_path, 'rb') as remote_file:
                # Leer todo el contenido para guardar copia exacta
                file_content = remote_file.read()

                try:
                    decoded = file_content.decode('utf-8', errors='ignore')
                except:
                    decoded = file_content.decode('latin-1', errors='ignore')
                
                # Volver al inicio para parsear
                log_info = self.parse_log_file(StringIO(decoded), filename, estacion_nombre)
            
            # Determinar proyecto (usar 'unknown' si no se puede determinar)
            project = self.determine_project(log_info)
            
            # Crear ruta completa local
            local_dir = base_dir / 'logs' / 'tim' / project / estacion_nombre
            local_path = local_dir / filename
            
            # Asegurar que el directorio existe
            local_dir.mkdir(parents=True, exist_ok=True)
            
            # Guardar copia exacta del archivo original
            with open(local_path, 'wb') as local_file:
                local_file.write(file_content)
            
            self.stdout.write(self.style.SUCCESS(
                f'Copia local guardada en: {local_path}'
            ))
            
            return log_info
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(
                f'Error guardando copia local {filename}: {str(e)}'
            ))
            raise

    def parse_log_file(self, remote_file, filename, estacion_nombre):
        """
        Extrae información del archivo de log para proyecto TIM
        """
        info = {
            'sn': '',
            'operator_id': None,
            'part_number': None,
            'log_datetime': None,
            'error_message': None,
            'station_id': None,
            """
            ============================================================================
            CAMBIO COMIENZO: Se elimina 'factory' ya que se determinará por SOAP
            ============================================================================
            """
            'raw_content': ''
        }
        
    # Metodo para probar ajustar GDL y no-GDL en base al operador-id
    #def determine_factory_from_operator(self, factory_value, operator_id):
    #    if not operator_id:
    #        return factory_value

    #    op = operator_id.upper().replace(" ", "")

    #    if "RMA" in op:
    #        return "NO-GDL"

    #    return "GDL"

        try:
            # Leer todo el contenido primero
            raw_content = remote_file.read()
            info['raw_content'] = raw_content
            
            # Volver al inicio para parsear línea por línea
            remote_file.seek(0)
            contenido_lineas = remote_file.readlines()

            # Extraer SN
            for linea in contenido_lineas:
                linea = linea.strip()

                if linea.startswith('[') and ']' in linea:
                    posible = linea.split(']')[0][1:]
                    if posible.startswith("FCR") and len(posible) >= 10:
                        info['sn'] = posible
                        break

            # Fallback SN - Solo si no se encontró en el contenido
            if not info['sn']:
                info['sn'] = self.extract_serial_from_filename(filename)

            # Extraer fecha/hora
            for linea in contenido_lineas:
                linea = linea.strip()

                if linea.startswith("LOG filename:") and "[" in linea and "]" in linea:
                    grupos = re.findall(r'\[(.*?)\]', linea)

                    if len(grupos) >= 2:
                        fecha_raw = grupos[1]
                        try:
                            partes = fecha_raw.split()
                            fecha_str = partes[0].replace("_", "-")
                            hora_str = ":".join(part.replace("_", ":") for part in partes[1:])
                            datetime_str = f"{fecha_str} {hora_str}"

                            info['log_datetime'] = datetime.strptime(
                                datetime_str, '%Y-%m-%d %H:%M:%S'
                            )
                        except:
                            pass
                    break

            # Fallback fecha
            if not info['log_datetime']:
                info['log_datetime'] = self.extract_datetime_from_filename(filename)

            # Extraer station ID
            info['station_id'] = self.determine_station_type(estacion_nombre)

            # Fallback station ID
            if not info['station_id']:
                info['station_id'] = self.determine_station_type(estacion_nombre) or 'UNKNOWN'

            # Extraer operator ID si está disponible y part number
            for linea in contenido_lineas:
                linea = linea.strip()

                if not info['operator_id'] and "OP id:" in linea:
                    info['operator_id'] = linea.split("OP id:")[1].strip()

                if not info['part_number'] and "Part Number:" in linea:
                    info['part_number'] = linea.split("Part Number:")[1].strip()[:12]

            """
            ============================================================================
            CAMBIO COMIENZO: Se elimina la extracción de "factory" del log
            Ya no se busca "Factory:" en el contenido del archivo
            ============================================================================

            ============================================================================
            CAMBIO FIN: Se elimina la extracción de "factory" del log
            ============================================================================
            """

            # Si es archivo FAIL, extraer mensaje de error estandarizado
            if "FAIL" in filename and not info['error_message']:
                info['error_message'] = self.extract_standardized_error(
                    info['raw_content'], info['station_id'], filename
                )

            return info

        except Exception as e:
            self.stdout.write(self.style.WARNING(f'Error leyendo archivo {filename}: {str(e)}'))
            return info

    def extract_standardized_error(self, raw_content, station_type, filename):
        """
        Convierte mensajes de error crudos a mensajes estandarizados
        """
        raw_content = raw_content.lower()

        if not hasattr(self, "compiled_error_patterns"):
            self.compiled_error_patterns = {}
        
        # Mapeo de patrones de error a mensajes estandarizados por estación
        error_patterns = {
            'BFT': [
                # Check version fail
                (r"check version fail", "Check version fail"),

                # BootROM data crash
                (r"(bootrom:\s*bad header at offset [0-9a-f]+\s*)+(?=[\s\S]*?(bootrom\s*1\.41|booting from nand flash|wait prompt.*timeout))", "BootROM data crash"),

                # Port Link down
                (r"port.*link down", "Port Link down"),

                # Fail LED test
                (r"operator fail led test", "Fail LED test"),

                # Linespeed fail port 1 - 48
                (r"linespeed.*fail.*port|FAILED: Linespeed test", "Linespeed fail port 1 - 48"),

                # Button reset test fail
                (r"checkreset.*timeout", "Button reset test fail"),

                # Root initiated a new r/w session
                (r"root.*initiated.*new.*r/w session[\s\S]{0,200}?(bootrom|booting from nand flash)[\s\S]{0,200}?wait prompt 'diag>'", "Root initiated a new r/w session"),

                # Telnet connection is abnormal
                (r"telnet connection is abnormal", "Telnet connection is abnormal"),

                # Wait prompt DIAG
                (r"root.*initiated.*new.*r/w session(?![\s\S]{0,200}?(bootrom|booting from nand flash))[\s\S]{0,200}?wait prompt 'diag>'", "Wait prompt DIAG"),

                # Run command fail
                (r"run command.*fail", "Run command fail"),

                # FAILED: SMI test, ERROR: data mismatch
                (r"failed:\s*SMI test[\s\S]{0,200}?error:\s*data mismatch|smitest\s+fail", "FAILED: SMI test, ERROR: data mismatch"),

                # Press x to choose XMODEM...
                (r"press x to choose xmodem\.\.\.[\s\S]*?please xmodem image file\s*\.\.\.[\s\S]*?wait prompt.*timeout", "Press x to choose XMODEM..."),

                # PSU Status Fail
                (r"run\s+'psustatus\s+\d+\s+\d+'\s+fail", "PSU Status Fail"),

                # Tempread All Fail
                (r"error:\s*write reg error[\s\S]*?tempread all fail", "Tempread All Fail"),

                # DRAM initialization failed
                (r"dram initialization failed|ddre.*failed", "DRAM initialization failed"),
            ],
            
            'RUNIN': [
                # Check temperature items fail
                (r"(check\s+temp(eratrue)?\s+(value\s+fail|items\s+fail)|temp\s+\d+\.\d+\s+is\s+out\s+of\s+range)", "Check temperature items fail"),

                # Port Link Down
                (r"port\s+\d+(,\d+)*\s+link\s+down", "Port Link Down"),

                # Memory Test Fail
                (r"(marchc memory test[\s\S]*?)iteration:\s*(\d+)[\s\S]*(run memtest marchc\s*.*)", "Memory Test Fail"),

                # Test Program Fail
                (r"run linespeed exts 49-50.*,after waiting (\d+) seconds", "Test Program Fail"),

                # LineSpeed fail ports
                (r"port 49.*50.*time out|linespeed.*port 49-50", "LineSpeed fail ports"),

                # Port 01 and 48 time out
                (r"failed: linespeed test port.*?\(\d+\s+test fail\)", "Port 01 and 48 SFP time out"),

                # I2C test all test fail
                (r"i2ctest all test fail", "I2C test all test fail"),

                # FAILED: MEMTEST test, ERROR: data mismatch
                (r"failed: memory test[\s\S]*?error:\s*data\s*mismatch|memtest.*test fail", "FAILED: MEMTEST test, ERROR: data mismatch"),

                ],
            
            'FCA': [
                # BootROM data crash
                (r"(bootrom: bad header at offset [0-9a-f]+\s+)+bootrom: trying uart", "BootROM data crash"),

                # ERROR: can't get kernel image!
                (r"can't get kernel image!", "ERROR: can't get kernel image!"),

                # root' initiated a new r/w session
                (r"root.*initiated.*new.*r/w session[\s\S]{0,100}?wait prompt 'diag>'|root.*initiated.*new.*r/w session[\s\S]{0,100}?fca end test time", "Root initiated a new r/w session"),

                # Press x to choose XMODEM...
                (r"running uboot[\s\S]{0,500}?resetting ?\.{2,}|running uboot.*?u-boot[\s\S]*?wait prompt 'marvell>>' timeout,after waiting 180 seconds", "Press x to choose XMODEM..."),

                # Fail Finish Boot
                (r"[kconsole#[\s\S]*check the active-image version fail", "Fail Finish Boot"),

                # Telnet Conection Fail
                (r"telnet connection is abnormal", "Telnet Conection Fail"),

                # Ping test fail
                (r"(gatewayip needed but not set|ehci timed out)[\s\S]*?(ping (failed; host [\d\.]+ is not alive|test fail))", "Ping test fail"),

                # Fail USB not detected
                (r"(mvegigainit:[\s\S]*?failed|mvnetaportenable failed)[\s\S]*?(ping (failed; host [\d\.]+ is not alive|test fail))", "Fail USB not detected"),

                # Run Command Fail
                (r"user name:\s*[\s\S]*?run command\s*'y'\s*time out[,a-z\s0-9]*", "Run Command Fail"),
                
            ]
        }
        
        # Compilar solo una vez por estación
        if station_type not in self.compiled_error_patterns:
            self.compiled_error_patterns[station_type] = [
                (re.compile(pattern, re.IGNORECASE | re.DOTALL), message)
                for pattern, message in error_patterns.get(station_type, [])
            ]

        # Buscar coincidencias usando los patrones compilados
        for compiled_pattern, standardized_message in self.compiled_error_patterns[station_type]:
            if compiled_pattern.search(raw_content):
                return standardized_message
        
        # Si no se encuentra patrón, usar fallback
        return self.extract_fallback_error(raw_content)

    def extract_fallback_error(self, raw_content):
        """
        Extrae mensaje de error cuando no se encuentra patrón específico
        """
        # Buscar líneas que contengan palabras clave de error
        error_keywords = ['fail', 'error', 'timeout', 'crash', 'bad', 'wrong']
        lines = raw_content.split('\n')
        
        for line in lines:
            line_lower = line.lower()
            if any(keyword in line_lower for keyword in error_keywords):
                # Limpiar y acortar el mensaje
                clean_line = line.strip()
                if len(clean_line) > 100:
                    clean_line = clean_line[:100] + "..."
                return clean_line
        
        # Último recurso: primeras líneas del contenido
        first_lines = '\n'.join(lines[:3])
        if len(first_lines) > 150:
            first_lines = first_lines[:150] + "..."
        return first_lines

    def determine_station_type(self, estacion_nombre):
        """
        Determina el tipo de estación basado en el nombre
        """
        if 'BFT' in estacion_nombre:
            return 'BFT'
        elif 'RUNIN' in estacion_nombre:
            return 'RUNIN'
        elif 'FCA' in estacion_nombre:
            return 'FCA'
        else:
            return 'UNKNOWN'

    def determine_project(self, log_info):
        """
        Determina el proyecto basado en el part number
        """
        if not log_info.get('part_number'):
            return 'unknown'
        
        try:
            boom = Booms.objects.filter(pn=log_info['part_number']).first()
            return boom.project.lower() if boom else 'unknown'
        except Exception as e:
            self.stdout.write(self.style.WARNING(
                f'Error al determinar proyecto para PN {log_info.get("part_number")}: {str(e)}'
            ))
            return 'unknown'

    def extract_datetime_from_filename(self, filename):
        """
        Extrae fecha/hora del nombre del archivo
        """
        try:
            # Formato: [FCR1F89N84112][2025_09_01 23_27_03]BFT[FAIL].txt
            match = re.search(r'\[(\d{4}_\d{2}_\d{2} \d{2}_\d{2}_\d{2})\]', filename)
            if match:
                datetime_str = match.group(1).replace('_', '-', 3).replace('_', ':', 2)
                return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        except (ValueError, AttributeError) as e:
            self.stdout.write(self.style.WARNING(
                f'Error extrayendo fecha de {filename}: {str(e)}'
            ))
        
        return timezone.now()

    def extract_serial_from_filename(self, filename):
        """
        Extrae número de serie del nombre del archivo
        """
        try:
            # Formato: [FCR1F89N84112][2025_09_01 23_27_03]BFT[FAIL].txt
            match = re.search(r'\[(FCR[A-Z0-9]+)\]', filename)
            if match:
                return match.group(1)
        except (AttributeError, ValueError) as e:
            self.stdout.write(self.style.WARNING(
                f'Error extrayendo SN de {filename}: {str(e)}'
            ))
        
        return ''

    """
    ============================================================================
    MODIFICACIÓN COMIENZO: Métodos de registro modificados para aceptar session_employee
    ============================================================================
    """
    
    def register_uut(self, log_info, is_pass, session_employee=None):
        """
        Registra o actualiza una UUT en la base de datos - MODIFICADO
        """
        try:
            # Primero verificar si la UUT ya existe
            existing_uut = Uut.objects.filter(sn=log_info['sn']).first()
        
            if existing_uut:
                # Si ya existe, no modificamos su estado, solo devolvemos el objeto
                self.stdout.write(self.style.WARNING(
                    f'UUT existente encontrada: {log_info["sn"]}. No se modificará el estado.'
                ))
                return existing_uut

            employee = None
            
            """
            ============================================================================
            MODIFICACIÓN COMIENZO: PRIORIDAD DE EMPLEADOS
            1. Operador del log (si existe y se encuentra)
            2. Empleado de la sesión (para RUNIN con app gráfica)
            3. None (para otras estaciones o sin sesión)
            ============================================================================
            """
            if log_info['operator_id']:
                try:
                    user = User.objects.get(username=log_info['operator_id'].strip())
                    employee = Employes.objects.get(employeeNumber=user)
                    self.stdout.write(self.style.SUCCESS(
                        f'Usando operador del log: {employee.employeeName}'
                    ))
                except (User.DoesNotExist, Employes.DoesNotExist):
                    self.stdout.write(self.style.WARNING(
                        f'Operador {log_info["operator_id"]} no encontrado'
                    ))
                    employee = session_employee  # Fallback a empleado de sesión
            else:
                employee = session_employee  # Usar empleado de sesión si no hay operador
            """
            ============================================================================
            MODIFICACIÓN FIN: PRIORIDAD DE EMPLEADOS
            ============================================================================
            """
            
            pn_b = None
            if log_info['part_number']:
                pn_b = Booms.objects.filter(pn=log_info['part_number']).first()
                if not pn_b:
                    self.stdout.write(self.style.WARNING(
                        f'Part Number {log_info["part_number"]} no encontrado en Booms'
                    ))
            
            uut, created = Uut.objects.update_or_create(
                sn=log_info['sn'],
                defaults={
                    'date': log_info['log_datetime'] or timezone.now(),
                    'employee_e': employee,  # Usar el empleado determinado
                    'pn_b': pn_b,
                    'status': not is_pass
                }
            )
            
            if created:
                employee_name = employee.employeeName if employee else "Sistema"
                self.stdout.write(self.style.SUCCESS(
                    f'Nueva UUT creada: {log_info["sn"]} por {employee_name}'
                ))
            
            return uut
            
        except Exception as e:
            raise ValueError(f'Error registrando UUT: {str(e)}')

    def register_test_history(self, uut, ip, estacion_nombre, log_info, is_pass, session_employee=None):
        """
        Registra el historial de pruebas - MODIFICADO
        """
        try:
            # Usar la estación de la IP, no del log
            station, _ = Station.objects.get_or_create(stationName=estacion_nombre)

            employee = None
            
            """
            ============================================================================
            MODIFICACIÓN: Misma lógica de prioridad que en register_uut
            ============================================================================
            """
            if log_info['operator_id']:
                try:
                    user = User.objects.get(username=log_info['operator_id'].strip())
                    employee = Employes.objects.get(employeeNumber=user)
                except (User.DoesNotExist, Employes.DoesNotExist):
                    employee = session_employee
            else:
                employee = session_employee
            """
            ============================================================================
            MODIFICACIÓN FIN: Misma lógica de prioridad que en register_uut
            ============================================================================
            """
            
            test_history = TestHistory.objects.create(
                uut=uut,
                station=station,
                employee_e=employee,  # Usar el empleado determinado
                status=is_pass,
                test_date=log_info['log_datetime'] or timezone.now()
            )
            
            employee_name = employee.employeeName if employee else "Sistema"
            self.stdout.write(self.style.SUCCESS(
                f'TestHistory registrado por: {employee_name}'
            ))
            
            return test_history
            
        except Exception as e:
            raise ValueError(f'Error registrando TestHistory: {str(e)}')

    def register_failure(self, uut, ip, estacion_nombre, log_info, session_employee=None):
        """
        Registra una falla en la base de datos - MODIFICADO
        """
        try:
            # Usar la estación de la IP, no del log
            station, _ = Station.objects.get_or_create(stationName=estacion_nombre)
            
            employee = None
            
            """
            ============================================================================
            MODIFICACIÓN: Misma lógica de prioridad que en register_uut
            ============================================================================
            """
            if log_info['operator_id']:
                try:
                    user = User.objects.get(username=log_info['operator_id'].strip())
                    employee = Employes.objects.get(employeeNumber=user)
                except (User.DoesNotExist, Employes.DoesNotExist):
                    employee = session_employee
            else:
                employee = session_employee
            """
            ============================================================================
            MODIFICACIÓN FIN: Misma lógica de prioridad que en register_uut
            ============================================================================
            """
            
            hour = log_info['log_datetime'].hour if log_info['log_datetime'] else timezone.now().hour
            shift = '1' if 6 <= hour < 14 else '2' if 14 <= hour < 22 else '3'
            
            error_message_obj = None
            if log_info['error_message']:
                try:
                    msgs = ErrorMessages.objects.filter(message=log_info['error_message'])
                
                    if msgs.exists():
                        # Si ya hay uno o varios → usar el primero (evita crash)
                        error_message_obj = msgs.first()
                    else:
                        pn_b = Booms.objects.filter(pn=log_info['part_number']).first() if log_info['part_number'] else None
                        error_message_obj, created = ErrorMessages.objects.get_or_create(
                            message=log_info['error_message'],
                            defaults={
                                'employee_e': employee,  # Usar el empleado determinado
                                'pn_b': pn_b,
                                'date': timezone.now()
                            }
                        )
                        if created:
                            employee_name = employee.employeeName if employee else "Sistema"
                            self.stdout.write(self.style.SUCCESS(
                                f'Nuevo mensaje de error registrado por: {employee_name}'
                            ))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(
                        f'Error creando ErrorMessage: {str(e)}'
                    ))
            
            failure = Failures.objects.create(
                id_s=station,
                sn_f=uut,
                failureDate=log_info['log_datetime'] or timezone.now(),
                id_er=error_message_obj,
                employee_e=employee,  # Usar el empleado determinado
                shiftFailure=shift,
                analysis='',
                rootCause='',
                status="False",
                defectSymptom=log_info.get('error_message', 'No especificado'),
                correctiveActions='',
                comments=f'Error detectado automáticamente desde estación {estacion_nombre}'
            )
            
            employee_name = employee.employeeName if employee else "Sistema"
            self.stdout.write(self.style.SUCCESS(
                f'Falla registrada por {employee_name} para SN: {uut.sn}'
            ))
            
            return failure
            
        except Exception as e:
            raise ValueError(f'Error registrando Falla: {str(e)}')
    """
    ============================================================================
    MODIFICACIÓN FIN: Métodos de registro modificados para aceptar session_employee
    ============================================================================
    """
        
    """
    ============================================================================
    CAMBIO COMIENZO: Métodos SOAP modificados para adaptarse a esta clase
    Incluye manejo mejorado de errores y conectividad
    ============================================================================
    """
    
    def check_network_connectivity(self):
        """
        Verifica si hay conectividad de red antes de intentar consultar SOAP
        """
        try:
            # Intentar hacer ping al servidor SOAP
            socket.setdefaulttimeout(5)  # 5 segundos timeout
            test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            test_socket.connect(("10.12.197.87", 9400))
            test_socket.close()
            self.stdout.write(self.style.SUCCESS("Conectividad de red verificada"))
            return True
        except (socket.timeout, socket.error, ConnectionRefusedError) as e:
            self.stdout.write(self.style.ERROR(f"Sin conectividad de red: {str(e)}"))
            return False
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error verificando conectividad: {str(e)}"))
            return False
    
    # Configuración del cliente SOAP
    def get_soap_client(self):
        """
        Obtiene el cliente SOAP configurado
        """
        url = "http://10.12.197.87:9400/tst/pmdu/?wsdl"
        client = Client(url, retxml=True, timeout=10)  # Timeout de 10 segundos
        d = dict(http='http://10.12.197.87:9400')
        client.set_options(proxy=d)
        return client

    def call_current_stage(self, sysserial):
        """
        Obtiene el flujo actual (stage) desde el servicio SOAP.
        
        Consulta el servicio SOAP usando el método current_stage y extrae
        el valor entre las etiquetas <currentEvent> y </currentEvent>.
        
        Parámetros:
        sysserial: Número de serie del equipo a consultar
        
        Retorna:
        Contenido entre <currentEvent> y </currentEvent> o None si no se encuentra
        """
        self.stdout.write(f"Consultando SOAP para serial: {sysserial}")
        try:
            client = self.get_soap_client()
            result = client.service.current_stage(sysserial)
            
            # Convertir bytes a string
            xml_str = result.decode('utf-8')
            
            # Extraer contenido entre <currentEvent> y </currentEvent>
            start_tag = "<currentEvent>"
            end_tag = "</currentEvent>"
            
            start_index = xml_str.find(start_tag)
            end_index = xml_str.find(end_tag)
            
            if start_index != -1 and end_index != -1:
                content = xml_str[start_index + len(start_tag):end_index]
                self.stdout.write(f"Stage obtenido del SOAP: {content}")
                return content
            else:
                self.stdout.write(self.style.WARNING(f"No se encontró currentEvent en respuesta SOAP para {sysserial}"))
                return None
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error en consulta SOAP para {sysserial}: {str(e)}'))
            raise  # Relanzar para manejo superior

    def verificar_historial_fallas_semana(self, serial_number):
        """
        Verifica si es la primera falla del equipo en la semana actual.
        
        Calcula el inicio de la semana (lunes) y consulta en la base de datos
        cuántas fallas ha tenido el equipo desde ese día.
        
        Parámetros:
        serial_number: Número de serie del equipo
        
        Retorna:
        True si es primera falla en la semana, False si ya falló antes
        """
        try:
            # Calcular inicio de semana (lunes)
            hoy = datetime.now()
            dias_desde_lunes = hoy.weekday()  # 0=lunes, 1=martes, ..., 6=domingo
            inicio_semana = hoy - timedelta(days=dias_desde_lunes)
            
            # Ajustar a inicio del día
            inicio_semana = inicio_semana.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Consultar base de datos - MODIFICADO para usar el modelo correcto
            # Buscar UUT con el serial_number y luego contar sus fallas
            uut = Uut.objects.filter(sn=serial_number).first()
            if uut:
                fallas_semana = TestHistory.objects.filter(
                    uut=uut,
                    status=False,  # FAIL es False
                    test_date__gte=inicio_semana,
                    test_date__lt=hoy
                ).count()
                
                self.stdout.write(f"Fallas encontradas para {serial_number} en la semana: {fallas_semana}")
                return fallas_semana == 0  # True si es primera falla
            else:
                # Si no existe UUT, es primera falla
                self.stdout.write(f"No existe UUT para {serial_number}, se considera primera falla")
                return True
                
        except Exception as e:
            self.stdout.write(self.style.WARNING(f'Error al verificar historial de fallas para {serial_number}: {e}'))
            return True  # Por seguridad, asumir primera falla si hay error

    def procesar_test_completo(self, serial_number, resultado_test, test_history_model=None):
        """
        Procesa un resultado de test aplicando las reglas de negocio.
        
        Esta es la función principal que integra toda la lógica:
        1. Obtiene flujo actual desde SOAP
        2. Aplica reglas según resultado y flujo
        3. Verifica historial si es necesario
        4. Determina stage final
        
        Parámetros:
        serial_number: Número de serie del equipo (ej: 'FCR1E59G81034')
        resultado_test: Resultado del test ('PASS' o 'FAIL')
        test_history_model: Modelo TestHistory (opcional)
        
        Retorna:
        Diccionario con resultados del procesamiento
        
        Reglas aplicadas:
        - PASS + flujo REPAIR → NO-GDL
        - FAIL + flujo REPAIR + primera falla semana → GDL
        - FAIL + flujo REPAIR + falla repetida semana → NO-GDL
        - FAIL + flujo no REPAIR → GDL
        """
        
        self.stdout.write(f"\n{'='*60}")
        self.stdout.write(f"INICIANDO PROCESAMIENTO SOAP PARA: {serial_number}")
        self.stdout.write(f"Resultado test recibido: {resultado_test}")
        self.stdout.write(f"{'='*60}")
        
        resultado_dict = {
            'serial': serial_number,
            'resultado_test': resultado_test,
            'flujo_obtenido': None,
            'stage_final': None,
            'accion': '',
            'registro_historial': False
        }
        
        # Paso 1: Obtener flujo actual desde SOAP
        flujo_actual = self.call_current_stage(serial_number)
        
        if flujo_actual is None:
            # IMPORTANTE: Si SOAP no reconoce el SN, NO asumimos GDL automáticamente
            # Levantamos una excepción para manejo superior
            raise ValueError(f"El servicio SOAP no reconoce el serial number: {serial_number}")
        
        resultado_dict['flujo_obtenido'] = flujo_actual
        self.stdout.write(f"Flujo obtenido del SOAP: '{flujo_actual}'")
        
        # Paso 2: Aplicar reglas de negocio
        resultado_upper = resultado_test.upper()
        flujo_upper = flujo_actual.upper()
        
        # Caso 1: PASS
        if resultado_upper == 'PASS':
            if flujo_upper == 'REPAIR':
                # PASS con flujo REPAIR → NO-GDL
                stage_final = 'NO-GDL'
                accion = "PASS con flujo REPAIR: manejar como NO-GDL"
            else:
                # PASS normal → GDL
                stage_final = 'GDL'
                accion = "PASS normal: manejar como GDL"
        
        # Caso 2: FAIL
        elif resultado_upper == 'FAIL':
            if flujo_upper == 'REPAIR':
                # FAIL con flujo REPAIR → verificar historial
                es_primera_falla = self.verificar_historial_fallas_semana(serial_number)
                
                if es_primera_falla:
                    # Primera falla en la semana → GDL
                    stage_final = 'GDL'
                    accion = "FAIL con flujo REPAIR (primera falla semana): manejar como GDL"
                else:
                    # Falla repetida → NO-GDL
                    stage_final = 'NO-GDL'
                    accion = "FAIL con flujo REPAIR (falla repetida): manejar como NO-GDL"
            else:
                # FAIL sin REPAIR → GDL
                stage_final = 'GDL'
                accion = "FAIL sin flujo REPAIR: manejar como GDL"
        
        # Caso 3: Resultado desconocido
        else:
            stage_final = 'GDL'  # Valor por defecto
            accion = f"Resultado desconocido '{resultado_test}': usando GDL por defecto"
            self.stdout.write(f"ADVERTENCIA: Resultado de test desconocido: '{resultado_test}'")
        
        resultado_dict['stage_final'] = stage_final
        resultado_dict['accion'] = accion
        self.stdout.write(f"Acción determinada: {accion}")
        self.stdout.write(f"Stage final: {stage_final}")
        
        # Paso 3: Registrar en historial si es falla
        if resultado_upper == 'FAIL':
            self.stdout.write(f"Es una falla, pero no se registra historial adicional aquí")
            resultado_dict['registro_historial'] = True
        
        self.stdout.write(f"{'='*60}")
        self.stdout.write(f"PROCESAMIENTO COMPLETADO PARA: {serial_number}")
        self.stdout.write(f"Stage final: {stage_final}")
        self.stdout.write(f"{'='*60}\n")
        
        return resultado_dict
    """
    ============================================================================
    CAMBIO FIN: Métodos SOAP modificados para adaptarse a esta clase
    ============================================================================
    """