"""
Compressor: Merges all bot scripts into a single all_in_one_bot.py
Uses base64 encoding to avoid any string escaping issues.
"""
import base64
import os

files_to_merge = [
    'arb_scanner.py',
    'calendar_spread_bot.py',
    'funding.py',
    'futurearbitrage.py',
    'spot_future.py',
]

output_file = 'all_in_one_bot.py'

# Read and base64-encode each file
encoded_bots = {}
for py_file in files_to_merge:
    if not os.path.exists(py_file):
        print(f"WARNING: {py_file} not found, skipping.")
        continue
    with open(py_file, 'r', encoding='utf-8') as f:
        code = f.read()
    encoded = base64.b64encode(code.encode('utf-8')).decode('ascii')
    encoded_bots[py_file] = encoded
    print(f"  Encoded {py_file} ({len(code)} chars -> {len(encoded)} b64 chars)")

# Build the output file
with open(output_file, 'w', encoding='utf-8') as f:
    f.write('#!/usr/bin/env python3\n')
    f.write('# -*- coding: utf-8 -*-\n')
    f.write('"""\n')
    f.write('ALL-IN-ONE ARBITRAGE BOT\n')
    f.write('========================\n')
    f.write('This file contains all 5 arbitrage bot strategies in one executable.\n')
    f.write('Run: python all_in_one_bot.py\n')
    f.write('"""\n\n')
    f.write('import base64\nimport sys\nimport os\n\n')

    # Write the encoded bots dictionary
    f.write('# Each bot script is base64-encoded to avoid any string escaping issues\n')
    f.write('BOTS = {\n')
    for name, b64 in encoded_bots.items():
        # Split b64 into 76-char lines for readability
        lines = [b64[i:i+76] for i in range(0, len(b64), 76)]
        f.write(f'    "{name}": (\n')
        for line in lines:
            f.write(f'        "{line}"\n')
        f.write(f'    ),\n')
    f.write('}\n\n')

    # Write nice display names
    f.write('BOT_DESCRIPTIONS = {\n')
    f.write('    "arb_scanner.py":        "Cash & Carry Arbitrage Scanner (AngelOne Smart API)",\n')
    f.write('    "calendar_spread_bot.py": "Calendar Spread Arbitrage Bot (NSE India)",\n')
    f.write('    "funding.py":            "Funding Rate Arbitrage Bot (Binance/Bybit)",\n')
    f.write('    "futurearbitrage.py":     "Calendar Futures Arbitrage Bot (Yahoo Finance)",\n')
    f.write('    "spot_future.py":         "Spot-Future Arbitrage Bot (NSE/YFinance)",\n')
    f.write('}\n\n')

    # Write the main menu and execution logic
    f.write(r'''
def main():
    print()
    print("=" * 60)
    print("  ALL-IN-ONE ARBITRAGE BOT LAUNCHER")
    print("=" * 60)
    print()
    print("  Available strategies:")
    print()

    keys = list(BOTS.keys())
    for i, name in enumerate(keys, 1):
        desc = BOT_DESCRIPTIONS.get(name, name)
        print(f"    {i}. {desc}")
        print(f"       [{name}]")
        print()

    print("=" * 60)

    while True:
        try:
            choice = input("\n  Enter the number of the bot to run (1-{}): ".format(len(keys)))
            idx = int(choice.strip()) - 1
            if 0 <= idx < len(keys):
                break
            print("  Invalid choice. Please enter a number between 1 and {}.".format(len(keys)))
        except ValueError:
            print("  Invalid input. Please enter a number.")
        except (EOFError, KeyboardInterrupt):
            print("\n  Goodbye!")
            sys.exit(0)

    selected = keys[idx]
    desc = BOT_DESCRIPTIONS.get(selected, selected)
    print()
    print("=" * 60)
    print(f"  Starting: {desc}")
    print("=" * 60)
    print()

    # Decode the base64-encoded source code
    source_code = base64.b64decode(BOTS[selected]).decode("utf-8")

    # Execute it in a namespace where __name__ == "__main__"
    # so the bot's if __name__ == "__main__": block fires
    exec_globals = {"__name__": "__main__", "__file__": selected}
    try:
        exec(compile(source_code, selected, "exec"), exec_globals)
    except SystemExit:
        pass
    except KeyboardInterrupt:
        print("\n\n  Bot stopped. Returning to menu...")
    except Exception as e:
        print(f"\n  Error running {selected}: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
''')

file_size = os.path.getsize(output_file)
print(f"\nSuccessfully created {output_file} ({file_size:,} bytes)")
print(f"Contains {len(encoded_bots)} bots, ready to run with: python {output_file}")
