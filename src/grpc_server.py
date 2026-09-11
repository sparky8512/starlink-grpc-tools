import base64
import datetime
import json
from typing import Any, Dict, Optional
import uuid
import requests
import os
import sys
import starlink_grpc


def main(coin: str = "BTC", value: float = 1.0, transaction_type: str = "buy"):
    """
    Entry point with two parameters:
    - coin: cryptocurrency symbol (default "BTC")
    - value: amount (default 1.0)
    """
    print(f"Coin: {coin}")
    print(f"Value: {value}")
    print(f"Transaction Type: {transaction_type}")

if __name__ == "__main__":
    # If arguments are provided, override defaults
    if len(sys.argv) >= 4:
        coin = sys.argv[1]
        transaction_type = sys.argv[3]
        try:
            value = float(sys.argv[2])
        except ValueError:
            print("Value must be a number. Using default 1.0")
            value = 1.0
        main(coin, value, transaction_type)
    else:
        # No arguments → use defaults
        main()
