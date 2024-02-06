import asyncio
import sys

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Usage: python main.py <test|start>")
        sys.exit(1)

    if sys.argv[1] == "start":
        from scripts.start import main

        asyncio.run(main())
    elif sys.argv[1] == "test":
        from test.generate_dataset import main

        asyncio.run(main())
