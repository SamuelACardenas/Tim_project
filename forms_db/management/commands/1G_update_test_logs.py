import os
import paramiko
import re
from pathlib import Path
from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import datetime
from django.conf import settings
from django.contrib.auth.models import User
from forms_db.models import Uut, TestHistory, Station, Employes, Booms, Failures, ErrorMessages
from io import StringIO
from ast import operator
from suds.client import Client
import logging
from xml.dom.minidom import parseString
from suds.plugin import MessagePlugin
from lxml import etree
import requests
import xml
import time
from datetime import datetime, timedelta
from django.db.models import Q

class Command(BaseCommand):
    help = 'Actualiza logs de prueba desde estaciones remotas para proyecto TIM'
    
    def handle(self, *args, **options):#######################################aqui
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
            usuario = estacion["usuario"]

            password = self.get_password(nombre)

            try:
                self.process_station(ip, nombre, usuario, password)
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Error en estación {nombre}: {str(e)}'))

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

    def process_station(self, ip, estacion_nombre, usuario, password):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            self.stdout.write(self.style.SUCCESS(
                f"Intentando conectar a {estacion_nombre} ({ip}) con usuario '{usuario}'"
            ))

            client.connect(ip, port=22, username=usuario, password=password, timeout=10)

            self.stdout.write(self.style.SUCCESS(
                f"Conectado exitosamente a {estacion_nombre} ({ip})"
            ))

            sftp = client.open_sftp()

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
                        self.process_single_file(sftp, archivo, ip, estacion_nombre)
                    except Exception as e:
                        self.stdout.write(self.style.WARNING(f'Error procesando {archivo}: {str(e)}'))
                else:
                    pass

            sftp.close()

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error de conexión con {ip}: {str(e)}'))
        finally:
            client.close()

    def process_single_file(self, sftp, filename, ip, estacion_nombre):
        remote_path = filename 
        remote_backup_path = f"processed/{filename}"
        is_pass = "PASS" in filename
        
        try:
            # 1. Primero guardar copia local del archivo original
            log_info = self.save_local_copy(sftp, remote_path, filename, estacion_nombre)
            
            # 2. Luego mover el archivo remoto
            #try:
            #    sftp.mkdir('C:/LOG/TIM/processed')
            #except:
            #    pass  # El directorio ya existe
            
            #sftp.rename(remote_path, remote_backup_path)
            
            # 3. Solo registrar en BD si es GDL
            factory_value = str(log_info.get('factory') or '')  # Convierte None a '' seguro

            # Prueba para usar operador_id para valor de GDL y no-GDL
            # factory_value = self.determine_factory_from_operator(factory_value, log_info.get('operator_id'))

            if not factory_value:
                self.stdout.write(self.style.WARNING(
                    f"Advertencia: 'factory' es None o vacío en {filename}"
                ))

            # Condicion para mandar el SN al metodo de conexion SOAP
            if log_info.get('sn'):
                resultado_test = 'PASS' if is_pass else 'FAIL'

                # Llamada a la función de procesamiento de reglas
                resultado_reglas = self.procesar_test_completo(
                    serial_number=log_info['sn'],
                    resultado_test=resultado_test,
                    test_history_model=TestHistory
                )

                factory_value = resultado_reglas['stage_final']

            if factory_value.upper() == 'GDL':
                if not log_info.get('sn'):
                    raise ValueError("No se pudo extraer el número de serie del log")

                uut = self.register_uut(log_info, is_pass)
                test_history = self.register_test_history(uut, ip, estacion_nombre, log_info, is_pass)
                
                if not is_pass:
                    self.register_failure(uut, ip, estacion_nombre, log_info)
                
                self.stdout.write(self.style.SUCCESS(
                    f"Procesado (GDL): {filename} | SN: {log_info['sn']} | {'PASS' if is_pass else 'FAIL'}"
                ))
            else:
                self.stdout.write(self.style.SUCCESS(
                    f"Procesado (No-GDL): {filename} | SN: {log_info.get('sn', 'N/A')}"
                ))
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(
                f'Error procesando archivo {filename}: {str(e)}'
            ))
            raise


    # Metodo para probar ajustar GDL y no-GDL en base al operador-id
    #def determine_factory_from_operator(self, factory_value, operator_id):
    #    if not operator_id:
    #        return factory_value

    #    op = operator_id.upper().replace(" ", "")

    #    if "RMA" in op:
    #        return "NO-GDL"

    #    return "GDL"


    def save_local_copy(self, sftp, remote_path, filename, estacion_nombre):
        """Guarda copia local exacta del archivo original y devuelve la información parseada"""
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
        """Extrae información del archivo de log para proyecto TIM"""
        info = {
            'sn': '',
            'operator_id': None,
            'part_number': None,
            'log_datetime': None,
            'error_message': None,
            'station_id': None,
            'factory': None,
            'raw_content': ''
        }
        
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

            # Fallback SN
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

            # Extraer factory
            for linea in contenido_lineas:
                linea = linea.strip()
                if "Factory:" in linea:
                    info['factory'] = linea.split("Factory:")[1].strip().upper()
                    break

            if not info['factory']:
                est = estacion_nombre.upper()

                if "RUNIN" in est or "FCA" in est or "BFT" in est:
                    info['factory'] = "GDL"
                else:
                    info['factory'] = "UNKNOWN"

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
        """Convierte mensajes de error crudos a mensajes estandarizados"""
        raw_content = raw_content.lower()
        
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
        
        # Buscar patrones según el tipo de estación
        patterns = error_patterns.get(station_type, [])
        for pattern, standardized_message in patterns:
            if re.search(pattern, raw_content, re.IGNORECASE | re.DOTALL):
                return standardized_message
        
        # Si no se encuentra patrón, usar fallback
        return self.extract_fallback_error(raw_content)

    def extract_fallback_error(self, raw_content):
        """Extrae mensaje de error cuando no se encuentra patrón específico"""
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
        """Determina el tipo de estación basado en el nombre"""
        if 'BFT' in estacion_nombre:
            return 'BFT'
        elif 'RUNIN' in estacion_nombre:
            return 'RUNIN'
        elif 'FCA' in estacion_nombre:
            return 'FCA'
        else:
            return 'UNKNOWN'

    def determine_project(self, log_info):
        """Determina el proyecto basado en el part number"""
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
        """Extrae fecha/hora del nombre del archivo"""
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
        """Extrae número de serie del nombre del archivo"""
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

    def register_uut(self, log_info, is_pass):
        """Registra o actualiza una UUT en la base de datos"""
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
            if log_info['operator_id']:
                try:
                    user = User.objects.get(username=log_info['operator_id'].strip())
                    employee = Employes.objects.get(employeeNumber=user)
                except (User.DoesNotExist, Employes.DoesNotExist):
                    employee = None
                    self.stdout.write(self.style.WARNING(
                    f'Empleado {log_info["operator_id"]} no encontrado'
                    ))
            
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
                    'employee_e': employee,
                    'pn_b': pn_b,
                    'status': not is_pass
                }
            )
            
            if created:
                self.stdout.write(self.style.SUCCESS(f'Nueva UUT creada: {log_info["sn"]}'))
            
            return uut
            
        except Exception as e:
            raise ValueError(f'Error registrando UUT: {str(e)}')

    def register_test_history(self, uut, ip, estacion_nombre, log_info, is_pass):
        """Registra el historial de pruebas"""
        try:
            # Usar la estación de la IP, no del log
            station, _ = Station.objects.get_or_create(stationName=estacion_nombre)

            employee = None
            if log_info['operator_id']:
                try:
                    user = User.objects.get(username=log_info['operator_id'].strip())
                    employee = Employes.objects.get(employeeNumber=user)
                except (User.DoesNotExist, Employes.DoesNotExist):
                    employee = None
                    self.stdout.write(self.style.WARNING(
                    f'Empleado {log_info["operator_id"]} no encontrado'
                    ))
            
            return TestHistory.objects.create(
                uut=uut,
                station=station,
                employee_e=employee,
                status=is_pass,
                test_date=log_info['log_datetime'] or timezone.now()
            )
            
        except Exception as e:
            raise ValueError(f'Error registrando TestHistory: {str(e)}')

    def register_failure(self, uut, ip, estacion_nombre, log_info):
        """Registra una falla en la base de datos"""
        try:
            # Usar la estación de la IP, no del log
            station, _ = Station.objects.get_or_create(stationName=estacion_nombre)
            
            employee = None
            if log_info['operator_id']:
                try:
                    user = User.objects.get(username=log_info['operator_id'].strip())
                    employee = Employes.objects.get(employeeNumber=user)
                except (User.DoesNotExist, Employes.DoesNotExist):
                    employee = None
                    self.stdout.write(self.style.WARNING(
                    f'Empleado {log_info["operator_id"]} no encontrado'
                    ))
            
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
                                'employee_e': employee,
                                'pn_b': pn_b,
                                'date': timezone.now()
                            }
                        )
                        if created:
                            self.stdout.write(self.style.SUCCESS(
                                f'Nuevo mensaje de error registrado: {log_info["error_message"]}'
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
                employee_e=employee,
                shiftFailure=shift,
                analysis='',
                rootCause='',
                status="False",
                defectSymptom=log_info.get('error_message', 'No especificado'),
                correctiveActions='',
                comments=f'Error detectado automáticamente desde estación {estacion_nombre}'
            )
            
            self.stdout.write(self.style.SUCCESS(
                f'Falla registrada para SN: {uut.sn} - {log_info.get("error_message", "Error no especificado")}'
            ))
            
            return failure
            
        except Exception as e:
            raise ValueError(f'Error registrando Falla: {str(e)}')
        

    # Configuración de logging (opcional)
    # logging.basicConfig(level=logging.INFO)
    # logging.getLogger('suds.client').setLevel(logging.DEBUG)
    # logger = logging.getLogger(__name__)

    def xmlpprint(self, xml):
        """
        Formatea un string XML para hacerlo más legible.
        
        Parámetros:
        xml: string XML a formatear
        
        Retorna:
        XML formateado con sangrías
        """
        return etree.tostring(etree.fromstring(xml), pretty_print=True)

    # Configuración del cliente SOAP
    url = "http://10.12.197.87:9400/tst/pmdu/?wsdl"
    client = Client(url, retxml=True)
    d = dict(http='http://10.12.197.87:9400')
    client.set_options(proxy=d)


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
        print(f"Consultando stage para serial: {sysserial}")
        result = self.client.service.current_stage(sysserial)
        
        # Convertir bytes a string
        xml_str = result.decode('utf-8')
        
        # Extraer contenido entre <currentEvent> y </currentEvent>
        start_tag = "<currentEvent>"
        end_tag = "</currentEvent>"
        
        start_index = xml_str.find(start_tag)
        end_index = xml_str.find(end_tag)
        
        if start_index != -1 and end_index != -1:
            content = xml_str[start_index + len(start_tag):end_index]
            print(f"Stage obtenido: {content}")
            return content
        
        return None

    def verificar_historial_fallas_semana(self, serial_number, test_history_model):
        """
        Verifica si es la primera falla del equipo en la semana actual.
        
        Calcula el inicio de la semana (lunes) y consulta en la base de datos
        cuántas fallas ha tenido el equipo desde ese día.
        
        Parámetros:
        serial_number: Número de serie del equipo
        test_history_model: Modelo Django TestHistory para consulta
        
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
            
            # Consultar base de datos
            fallas_semana = test_history_model.objects.filter(
                serial_number=serial_number,
                resultado='FAIL',
                fecha_test__gte=inicio_semana,
                fecha_test__lt=hoy
            ).count()
            
            print(f"Fallas encontradas para {serial_number} en la semana: {fallas_semana}")
            
            return fallas_semana == 0  # True si es primera falla
            
        except Exception as e:
            print(f"Error al verificar historial de fallas: {e}")
            return True  # Por seguridad, asumir primera falla si hay error

    def procesar_test_completo(self, serial_number, resultado_test, test_history_model=None):
        """
        Procesa un resultado de test aplicando las reglas de negocio.
        
        Esta es la función principal que integra toda la lógica:
        1. Obtiene flujo actual desde SOAP
        2. Aplica reglas según resultado y flujo
        3. Verifica historial si es necesario
        4. Determina stage final
        5. Registra en historial si es falla
        
        Parámetros:
        serial_number: Número de serie del equipo (ej: 'GDL180065')
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
        
        print(f"\n{'='*60}")
        print(f"INICIANDO PROCESAMIENTO PARA: {serial_number}")
        print(f"Resultado test recibido: {resultado_test}")
        print(f"{'='*60}")
        
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
            print(f"ADVERTENCIA: No se pudo obtener flujo para {serial_number}")
            flujo_actual = 'NO_DISPONIBLE'
        
        resultado_dict['flujo_obtenido'] = flujo_actual
        print(f"Flujo obtenido del SOAP: '{flujo_actual}'")
        
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
                if test_history_model:
                    es_primera_falla = self.verificar_historial_fallas_semana(
                        serial_number, 
                        test_history_model
                    )
                    
                    if es_primera_falla:
                        # Primera falla en la semana → GDL
                        stage_final = 'GDL'
                        accion = "FAIL con flujo REPAIR (primera falla semana): manejar como GDL"
                    else:
                        # Falla repetida → NO-GDL
                        stage_final = 'NO-GDL'
                        accion = "FAIL con flujo REPAIR (falla repetida): manejar como NO-GDL"
                else:
                    # Sin modelo de historial → tratar como primera falla (GDL)
                    stage_final = 'GDL'
                    accion = "FAIL con flujo REPAIR (sin historial): manejar como GDL"
            else:
                # FAIL sin REPAIR → GDL
                stage_final = 'GDL'
                accion = "FAIL sin flujo REPAIR: manejar como GDL"
        
        # Caso 3: Resultado desconocido
        else:
            stage_final = 'GDL'  # Valor por defecto
            accion = f"Resultado desconocido '{resultado_test}': usando GDL por defecto"
            print(f"ADVERTENCIA: Resultado de test desconocido: '{resultado_test}'")
        
        resultado_dict['stage_final'] = stage_final
        resultado_dict['accion'] = accion
        print(f"Acción determinada: {accion}")
        print(f"Stage final: {stage_final}")
        
        # Paso 3: Registrar en historial si es falla
        if resultado_upper == 'FAIL' and test_history_model:
            try:
                from django.utils import timezone
                registro = test_history_model.objects.create(
                    serial_number=serial_number,
                    resultado='FAIL',
                    flujo=flujo_actual,
                    stage_utilizado=stage_final,
                    fecha_test=timezone.now(),
                    accion=accion
                )
                resultado_dict['registro_historial'] = True
                print(f"Registro de falla guardado en TestHistory. ID: {registro.id}")
            except Exception as e:
                print(f"ERROR al guardar en historial: {e}")
        
        print(f"{'='*60}")
        print(f"PROCESAMIENTO COMPLETADO PARA: {serial_number}")
        print(f"Stage final: {stage_final}")
        print(f"{'='*60}\n")
        
        return resultado_dict