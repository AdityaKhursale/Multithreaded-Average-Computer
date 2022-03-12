import random
import sys

if len(sys.argv) != 2:
    count = 100
else:
    count = sys.argv[1]

fname = "data/numbers.txt"
with open(fname, 'w') as f:
    for i in range(count):
        number = random.randint(1, 9223372036854775807)
        f.write(str(number) + ' ')

with open(fname, 'r') as f:
    numbers = f.readlines()

if len(numbers) != 1:
    raise Exception("File contains more than 1 line")

total = 0
counter = 0
for number in numbers[0].split():
    total += int(number)
    counter += 1

print("Expected below with your go code")
print("Total: {}".format(total))
print("Total Numbers: {}".format(counter))
print("Average: {}".format(float(total) / counter))
