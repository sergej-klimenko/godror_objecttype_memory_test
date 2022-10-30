
import sys
from collections import OrderedDict

ALLOCATED_STRING = ": allocated "
FREED_STRING = ": freed "

OCI_ALLOCATED_STRING = ": OCI allocated "
OCI_FREED_STRING = ": OCI freed "

memoryLocations = OrderedDict()

for line in open(sys.argv[1]):

    # check for ODPI-C allocation
    pos = line.find(ALLOCATED_STRING)
    if pos > 0:
        parts = line[pos + len(ALLOCATED_STRING):].split()
        size = int(parts[0])
        memoryLocation = parts[3]
        memoryLocations[memoryLocation] = size
        continue

    # check for ODPI-C free
    pos = line.find(FREED_STRING)
    if pos > 0:
        parts = line[pos + len(FREED_STRING):].split()
        memoryLocation = parts[2]
        del memoryLocations[memoryLocation]
        continue

    # check for OCI allocation
    pos = line.find(OCI_ALLOCATED_STRING)
    if pos > 0:
        parts = line[pos + len(OCI_ALLOCATED_STRING):].split()
        size = int(parts[0])
        memoryLocation = parts[3]
        memoryLocations[memoryLocation] = size
        continue

    # check for OCI free
    pos = line.find(OCI_FREED_STRING)
    if pos > 0:
        parts = line[pos + len(OCI_FREED_STRING):].split()
        memoryLocation = parts[2]
        del memoryLocations[memoryLocation]
        continue

for memoryLocation, size in memoryLocations.items():
    print(memoryLocation, "-> size", size)
