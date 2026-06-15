"""Kraken exchange reader."""

import csv
import datetime
import decimal
from pathlib import Path
from typing import Optional

import log_config
from core import kraken_asset_map

from .base import ExchangeReader
from .utils import force_decimal, parse_utc_time, xdecimal

log = log_config.getLogger(__name__)


class KrakenReader(ExchangeReader):
    """Reader for Kraken CSV files."""

    def __init__(self):
        super().__init__("kraken")
        self.operation_mapping = {
            "spend": "Sell",  # Sell ordered via 'Buy Crypto' button
            "receive": "Buy",  # Buy ordered via 'Buy Crypto' button
            "reward": "StakingInterest",
            "staking": "StakingInterest",
            "deposit": "Deposit",
            "withdrawal": "Withdrawal",
        }

    def read_file(self, file_path: Path, book) -> None:
        """Read Kraken CSV file."""
        fee_sign_of_file: Optional[bool] = None

        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # Skip header.
            next(reader)

            for columns in reader:

                num_columns = len(columns)
                # Kraken ledgers export format from October 2020 and ongoing
                if num_columns == 10:
                    (
                        txid,
                        refid,
                        _utc_time,
                        _type,
                        subtype,
                        aclass,
                        _asset,
                        _amount,
                        _fee,
                        balance,
                    ) = columns

                # Kraken ledgers export format from September 2020 and before
                elif num_columns == 9:
                    (
                        txid,
                        refid,
                        _utc_time,
                        _type,
                        aclass,
                        _asset,
                        _amount,
                        _fee,
                        balance,
                    ) = columns
                else:
                    log.error(
                        f"{file_path}: Unknown Kraken ledgers format: "
                        "Number of rows do not match known versions."
                    )
                    raise RuntimeError

                row = reader.line_num

                # Parse data.
                utc_time = parse_utc_time(_utc_time, "%Y-%m-%d %H:%M:%S")
                change = force_decimal(_amount)
                # remove the appended .S for staked assets
                _asset = _asset.removesuffix(".S")
                coin = kraken_asset_map.get(_asset, _asset)
                fee = force_decimal(_fee)
                # An older implementation expected always positive fees
                # It seems that newer ledger files can have negative fee
                # values instead.
                if fee != 0:
                    # As soon as the first fee!=0 appears, check whether the
                    # fees are positive or negative. All fees in the file
                    # should have the same sign.
                    if fee_sign_of_file is None:
                        fee_sign_of_file = fee < 0
                    # Adjust the fee sign so that fees are always positive.
                    if fee_sign_of_file is True:
                        fee *= -1
                    if fee < 0:
                        log.error(
                            f"{file_path} row {row}: Unexpected fee sign. "
                            "All fees should have the same sign. "
                            "Please create an Issue or PR."
                        )
                        raise RuntimeError

                operation = self.operation_mapping.get(_type)
                if operation is None:
                    if _type == "trade":
                        operation = "Sell" if change < 0 else "Buy"
                    elif _type in ["margin trade", "rollover", "settled", "margin"]:
                        log.error(
                            f"{file_path} row {row}: Margin trading is currently not "
                            "supported. Please create an Issue or PR."
                        )
                        raise RuntimeError
                    elif _type == "transfer":
                        if num_columns == 9:
                            # for backwards compatibility assume Airdrop for staking
                            log.warning(
                                f"{file_path} row {row}: Staking is not supported for"
                                "old Kraken ledger formats. "
                                "Please create an Issue or PR."
                            )
                            operation = "Airdrop"
                        elif subtype == "stakingfromspot":
                            operation = "Staking"
                        elif subtype == "stakingtospot":
                            operation = "StakingEnd"
                        elif subtype in ["spottostaking", "spotfromstaking"]:
                            # duplicate entries for staking actions
                            continue
                        else:
                            log.error(
                                f"{file_path} row {row}: Order subtype '{subtype}' is "
                                "currently not supported. Please create an Issue or PR."
                            )
                            raise RuntimeError
                    else:
                        log.error(
                            f"{file_path} row {row}: Other order type '{_type}' is "
                            "currently not supported. Please create an Issue or PR."
                        )
                        raise RuntimeError
                change = abs(change)

                # Validate data.
                assert operation
                assert coin
                assert change

                # Skip duplicate entries for deposits / withdrawals and additional
                # deposit / withdrawal lines for staking / unstaking / staking reward
                # actions.
                # The second deposit and the first withdrawal need to be considered,
                # since these are the points in time where the user actually has the
                # assets at their disposal. The first deposit and second withdrawal are
                # in the public trade history and are skipped.
                # For staking / unstaking / staking reward actions, deposits /
                # withdrawals only occur once and will be ignored.
                # The "appended" flag stores if an operation for a given refid has
                # already been appended to the operations list:
                # == None: Initial value (first occurrence)
                # == False: No operation has been appended (second occurrence)
                # == True: Operation has already been appended, this should not happen
                if operation in ["Deposit", "Withdrawal"]:
                    # First, create the operations
                    op = book._create_operation(
                        operation, utc_time, self.platform, change, coin, row, file_path
                    )
                    op_fee = None
                    if fee != 0:
                        op_fee = book._create_operation(
                            "Fee", utc_time, self.platform, fee, coin, row, file_path
                        )
                    if (op is None) or (fee != 0 and op_fee is None):
                        # Ignore on this run - operation could not be created
                        # This might lead to unexpected errors while parsing the
                        # rest of the file...
                        # It'll be fixed, when the _missing_operation_mappings
                        # aren't missing.
                        pass
                    # If this is the first occurrence, set the "appended" flag to false
                    # and don't append the operation to the list. Instead, store the
                    # data for verifying or appending it later.
                    elif book.kraken_held_ops[refid]["appended"] is None:
                        book.kraken_held_ops[refid]["appended"] = False
                        book.kraken_held_ops[refid]["operation"] = op
                        book.kraken_held_ops[refid]["operation_fee"] = op_fee
                    # If this is the second occurrence, append a new operation, set the
                    # "appended" flag to True and assert that the data of this operation
                    # agrees with the data of the first occurrence.
                    elif book.kraken_held_ops[refid]["appended"] is False:
                        book.kraken_held_ops[refid]["appended"] = True
                        try:
                            # Make sure, that the found operations with the
                            # same refid  have the same operation type, amount
                            # of change and same coin.
                            assert isinstance(
                                op, type(book.kraken_held_ops[refid]["operation"])
                            ), (
                                "operation "
                                f"({op.type_name} != "
                                f'{book.kraken_held_ops[refid]["operation"].type_name})'
                            )
                            assert (
                                op.change
                                == book.kraken_held_ops[refid]["operation"].change
                            ), (
                                "change "
                                f"({op.change} != "
                                f'{book.kraken_held_ops[refid]["operation"].change})'
                            )
                            assert (
                                op.coin == book.kraken_held_ops[refid]["operation"].coin
                            ), (
                                "coin "
                                f"({op.coin} != "
                                f'{book.kraken_held_ops[refid]["operation"].coin})'
                            )
                        except AssertionError as e:
                            log.error(
                                f"{file_path} row {row}: Operations with refid {refid} "
                                "do not match. This should not happen. "
                                "Please create an Issue or PR."
                            )
                            raise e
                        # For deposits, this is all we need to do before appending the
                        # operation. For withdrawals, we need to append the first
                        # withdrawal as soon as the second withdrawal occurs. Therefore,
                        # overwrite the operation with the stored first withdrawal.
                        if operation == "Withdrawal":
                            op = book.kraken_held_ops[refid]["operation"]
                        # Finally, append the operations and delete the stored
                        # operations to reduce memory consumption
                        book._append_operation(op)
                        if op_fee:
                            book._append_operation(op_fee)
                        del book.kraken_held_ops[refid]["operation"]
                        del book.kraken_held_ops[refid]["operation_fee"]
                    # If an operation with the same refid has been already appended,
                    # this is the third occurrence. Throw an error if this happens.
                    elif book.kraken_held_ops[refid]["appended"] is True:
                        log.error(
                            f"{file_path} row {row}: More than two entries with refid "
                            f"{refid} should not exist ({operation}). "
                            "Please create an Issue or PR."
                        )
                        raise RuntimeError
                    # This should never happen
                    else:
                        log.error(
                            f"{file_path} row {row}: Unknown value for appended "
                            f"operation flag {book.kraken_held_ops[refid]['appended']}."
                            "Please create an Issue or PR."
                        )
                        raise TypeError

                # for all other operation types
                else:
                    self.append_operation(
                        book, operation, utc_time, change, coin, row, file_path
                    )
                    if fee != 0:
                        self.append_operation(
                            book, "Fee", utc_time, fee, coin, row, file_path
                        )
                    if operation == "StakingInterest":
                        # For Kraken, the rewarded coins are added to the staked
                        # portfolio. TODO (for MULTI_DEPOT only): Directly add the
                        # rewarded coins to the staking depot (not like here with the
                        # detour of adding it to spot and then staking the same amount)
                        self.append_operation(
                            book, "Staking", utc_time, change, coin, row, file_path
                        )
