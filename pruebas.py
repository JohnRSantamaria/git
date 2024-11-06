#!/bin/python3

import math
import os
import random
import re
import sys
from typing import Dict, Optional, Tuple, Any

Action = str

class State:
    # Implement the State class here
    def __init__(self, name: str):      
        self.name = name
        
    def __repr__(self):
        return self.name

# Implement the init_state here
unauthorized = State("unauthorized")
authorized = State("authorized")
init_state = unauthorized

def check_login(param, password, balance):
    return param == password, balance, "authorized" if param == password else "unauthorized"

def check_deposit(amount, password, balance):
    return True, balance + amount, f"Deposited {amount}"

def check_withdraw(amount, password, balance): 
    if amount <= balance:
        return True, balance - amount, f"Withdrew {amount}"
    return False, balance - "Insuffient founds"

def check_logut(param, password, balance):
    return True, balance, "unauthorized"

# Implement the transition_table here
transition_table = {
    unauthorized: [
        ("login", check_login, authorized)
    ],
    authorized: [
        ("deposit", check_deposit, authorized),
        ("withdraw", check_withdraw, authorized),
        ("logout", check_logut, unauthorized)
    ]
}

# Look for the implementation of the ATM class in the below Tail section
class ATM:
    def __init__(self, init_state: State, init_balance: int, password: str, transition_table: Dict):
        self.state = init_state
        self._balance = init_balance
        self._password = password
        self._transition_table = transition_table
        
    def next(self, action: Action, param: Optional) -> Tuple[bool, Optional[Any]]: # type: ignore
        try:
            for transition_action, check, next_state in self._transition_table[self.state]:
                if action == transition_action:
                    passed, new_balance, res = check(param, self._password, self._balance)
                    if passed:
                        self._balance = new_balance
                        self._state = next_state
                        return True, res
        except KeyError:
            pass
        return False, None
    

if __name__ == "__main__":
    class ATM:
        def __init__(self, init_state: State, init_balance: int, password: str, transition_table: Dict):
            self.state = init_state
            self._balance = init_balance
            self._password = password
            self._transition_table = transition_table

        def next(self, action: Action, param: Optional) -> Tuple[bool, Optional[Any]]: # type: ignore
            try:
                for transition_action, check, next_state in self._transition_table[self.state]:
                    if action == transition_action:
                        passed, new_balance, res = check(param, self._password, self._balance)
                        if passed:
                            self._balance = new_balance
                            self.state = next_state
                            return True, res
            except KeyError:
                pass
            return False, None


    if __name__ == "__main__":
        fptr = open(os.environ['OUTPUT_PATH'], 'w')
        password = input()
        init_balance = int(input())
        atm = ATM(init_state, init_balance, password, transition_table)
        q = int(input())
        for _ in range(q):
            action_input = input().split()
            action_name = action_input[0]
            try:
                action_param = action_input[1]
                if action_name in ["deposit", "withdraw"]:
                    action_param = int(action_param)
            except IndexError:
                action_param = None
            success, res = atm.next(action_name, action_param)
            if res is not None:
                fptr.write(f"Success={success} {atm.state} {res}\n")
            else:
                fptr.write(f"Success={success} {atm.state}\n")

        fptr.close()
# ---------------------------------------------------------------------------------------------------------------------
class ATM:
    def __init__(self, init_state: State, init_balance: int, password: str, transition_table: Dict):
        self.state = init_state
        self._balance = init_balance
        self._password = password
        self._transition_table = transition_table

    def next(self, action: Action, param: Optional) -> Tuple[bool, Optional[Any]]:
        if action == "balance":
            if self.state == authorized:
                return True, self._balance
            return False, None

        try:
            for transition_action, check, next_state in self._transition_table[self.state]:
                if action == transition_action:
                    passed, new_balance, error_message = check(param, self._password, self._balance)
                    if passed:
                        self._balance = new_balance
                        self.state = next_state
                        return True, None
                    return False, error_message
        except KeyError:
            pass
        return False, None

if __name__ == "__main__":
    password = input()
    init_balance = int(input())
    atm = ATM(init_state, init_balance, password, transition_table)
    q = int(input())
    for _ in range(q):
        action_input = input().split()
        action_name = action_input[0]
        try:
            action_param = action_input[1]
            if action_name in ["deposit", "withdraw"]:
                action_param = int(action_param)
        except IndexError:
            action_param = None

        success, res = atm.next(action_name, action_param)
        if action_name == "balance" and success:
            print(f"Success={success} {atm.state} {res}")
        elif res is not None:
            print(f"Success={success} {atm.state} {res}")
        else:
            print(f"Success={success} {atm.state}")
# ---------------------------------------------------------------------------------------------------------------------