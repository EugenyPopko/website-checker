import argparse
import os
from producer import SiteChecker



def get_args():
    parser = argparse.ArgumentParser(description="Check website availability",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--site", type=str, required=True,
                        help="site adress")
    parser.add_argument("--regexp", type=str, required=True,
                        help="regexp to find on site")
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    checker = SiteChecker()
    checker.check_site(args.site, args.regexp)
    

if __name__ == '__main__':
    main()