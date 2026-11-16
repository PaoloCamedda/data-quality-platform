import os
import platform
import sys
from pyspark.sql import SparkSession
from src.config import logger


def _find_java_home() -> str | None:
    """Rileva automaticamente il percorso di Java 17 in base al sistema operativo."""
    system = platform.system()

    if system == "Windows":
        base_dir = r"C:\Program Files\Eclipse Adoptium"
        if os.path.exists(base_dir):
            jdks = [d for d in os.listdir(base_dir) if d.startswith("jdk-17")]
            if jdks:
                return os.path.join(base_dir, jdks[0])

    elif system == "Darwin":  # macOS
        # Controlla i path standard di macOS (Adoptium / Homebrew)
        mac_paths = [
            "/Library/Java/JavaVirtualMachines",
            os.path.expanduser("~/Library/Java/JavaVirtualMachines"),
            "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
            "/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
        ]
        for base in mac_paths:
            if os.path.exists(base):
                if "openjdk@17" in base or "Home" in base:
                    if os.path.exists(base):
                        return base
                else:
                    subdirs = [os.path.join(base, d, "Contents", "Home") for d in os.listdir(base) if "17" in d]
                    for sub in subdirs:
                        if os.path.exists(sub):
                            return sub

    # Linux / Docker o fallback sulle variabili d'ambiente di sistema
    return os.environ.get("JAVA_HOME")


def get_spark_session() -> SparkSession:
    """Inizializza o recupera una SparkSession configurata per Windows, macOS, Linux e venv."""
    java_home = _find_java_home()
    if java_home and os.path.exists(java_home):
        os.environ["JAVA_HOME"] = java_home
        bin_path = os.path.join(java_home, "bin")
        if bin_path not in os.environ.get("PATH", ""):
            os.environ["PATH"] = bin_path + os.pathsep + os.environ.get("PATH", "")

    # Forza PySpark a usare l'interprete Python dell'ambiente virtuale corrente
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    logger.info(f"Inizializzazione SparkSession (OS: {platform.system()}, JAVA_HOME={os.environ.get('JAVA_HOME', 'default')})")
    
    spark = (
        SparkSession.builder
        .appName("CSVDataQualityPlatform")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    logger.info(f"SparkSession attiva (Versione Spark: {spark.version})")
    return spark