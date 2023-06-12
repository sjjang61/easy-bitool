import argparse
import uvicorn

def arg_parse():
    parser = argparse.ArgumentParser(description='easy-bitool')
    # parser.add_argument('-e', '--env', nargs='?', default='local', help='PRD, STG, DEV, local')
    return parser.parse_args()


if __name__ == "__main__":

    args = arg_parse()
    # os.environ['APP_ENV'] = args.env
    # settings = load_setting()
    uvicorn.run("main:app", host="0.0.0.0", port=30000)