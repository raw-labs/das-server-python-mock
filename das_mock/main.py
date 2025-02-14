import sys
from das_mock.server import serve

def main():
    port = 50051
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    serve(port=port)

if __name__ == "__main__":
    main()