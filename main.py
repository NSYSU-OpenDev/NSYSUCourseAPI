import sys

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Usage: python main.py <start|evaluate>")
        sys.exit(1)

    if sys.argv[1] == "start":
        from scripts.API_generation import start

        start()
    elif sys.argv[1] == "evaluate":
        from scripts.evaluate_captcha_model import start

        start()
