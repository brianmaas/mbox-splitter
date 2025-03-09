#!/usr/bin/env python
# -*- coding: utf-8 -*-

import mailbox
import sys
import gc
from os.path import exists

if len(sys.argv) != 3:
    print("\nUsage: `python mbox-splitter.py filename.mbox size`")
    print("         where `size` is a positive integer in Mb\n")
    print("Example: `python mbox-splitter.py inbox_test.mbox 50`")
    print("         where inbox_test.mbox size is about 125 Mb\n")
    print("Result:")
    print("Created file `inbox_test_1.mbox`, size=43Mb, messages=35")
    print("Created file `inbox_test_2.mbox`, size=44Mb, messages=2")
    print("Created file `inbox_test_3.mbox`, size=30Mb, messages=73")
    print("Done")
    sys.exit()

filename = sys.argv[1]
if not exists(filename):
    print(f"File `{filename}` does not exist.")
    sys.exit()

try:
    split_size = int(sys.argv[2]) * 1024 * 1024  # Convert MB to bytes
except ValueError:
    print("Size must be a positive number")
    sys.exit()

if split_size < 1:
    print("Size must be a positive number")
    sys.exit()

# Open the source mailbox once
source_mbox = mailbox.mbox(filename)
if len(source_mbox) == 0:
    print(f"Email messages in `{filename}` not found.")
    sys.exit()

chunk_count = 1
output = filename.replace('.mbox', f'_{chunk_count}.mbox')
if exists(output):
    print(f"The file `{filename}` has already been split. Delete chunks to continue.")
    sys.exit()

print(f"Splitting `{filename}` into chunks of {sys.argv[2]}Mb ...\n")

total_size = 0
of = mailbox.mbox(output, create=True)
chunk_message_count = 0  # Count of messages in the current chunk
global_count = 0         # Total messages processed

for message in source_mbox:
    global_count += 1
    chunk_message_count += 1

    # Print an update every 100 messages
    if global_count % 100 == 0:
        print(f"Processed {global_count} messages so far...", end='\r')
        sys.stdout.flush()
        gc.collect()  # Force garbage collection to help with memory

    message_bytes = message.as_bytes()
    message_size = len(message_bytes)

    if total_size + message_size >= split_size:
        of.flush()
        of.close()
        print(f"Created file `{output}`, size={total_size / (1024 * 1024):.2f}Mb, messages={chunk_message_count}.")

        chunk_count += 1
        total_size = 0
        output = filename.replace('.mbox', f'_{chunk_count}.mbox')
        of = mailbox.mbox(output, create=True)
        chunk_message_count = 0

    of.add(message)
    total_size += message_size

# Write out the last chunk
of.flush()
of.close()
print(f"Created file `{output}`, size={total_size / (1024 * 1024):.2f}Mb, messages={chunk_message_count}.")
print(f"\nTotal messages processed: {global_count}")
print("Done")
