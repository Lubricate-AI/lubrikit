import logging
import os

# Configure logging for development - users can override this
if os.getenv("LUBRIKIT_DEV", "").lower() in ("1", "true", "yes"):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

# Add NullHandler to prevent logs if no handler is configured by user
logging.getLogger(__name__).addHandler(logging.NullHandler())
