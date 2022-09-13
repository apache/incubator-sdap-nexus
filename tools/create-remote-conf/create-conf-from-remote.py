import argparse
import requests
import yaml


def parse_args():
    parser = argparse.ArgumentParser(description='Create a collections.yaml config from a remote SDAP',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--remote-nexus-list',
                        help='The url of the remote SDAP server, list end-point',
                        required=True
    )

    parser.add_argument('--prefix',
                        help='Add prefix to the remote collection id to form the local collection id',
                        required=False,
                        default="")

    return parser.parse_args()


def main():
    the_args = parse_args()
    r = requests.get(the_args.remote_nexus_list, verify=False)
    collection_config = []
    for collection in r.json():
        if 'remoteUrl' not in collection:
            collection_config.append(
                {
                    'id': the_args.prefix + collection['shortName'],
                    'path': the_args.remote_nexus_list.removesuffix('/list'),
                    'remote-id': collection['shortName']
                }
            )

    with open('./collections.yml', 'w') as outfile:
        yaml.dump(collection_config, outfile, default_flow_style=False)






if __name__ == '__main__':
    main()