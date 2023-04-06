# import libraries and functions to the python application
from src._run_scripts_ import *

logger = logging.getLogger("TwaiceSanojSolution")

# Define logg pattern
formatter = logging.Formatter(fmt='%(asctime)s %(name)s %(levelname)s: %(message)s (%(filename)s:%(lineno)d)',datefmt='%Y-%m-%d %H:%M:%S')
logger.setLevel(logging.INFO)
console = logging.StreamHandler()
console.setFormatter(formatter)
logger.addHandler(console)

logger.info("start the application")

try:
    SensorDataEnrichment("/app/input","/app/output").run()
except Exception:
    logger.error(msg = "falied application SensorDataEnrichment", exc_info = True)


try:
    CumulativeThroughput("/app/output", "/app/output").run()
except Exception:
    logger.error(msg = "falied application CumulativeThroughput", exc_info = True)

logger.info("successfully completed the application")