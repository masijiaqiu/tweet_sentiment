
fin = open("local_output.txt")
correct = 0
total = 0
for line in fin.readlines():
    items = line.strip().split()
    pred = items[3]
    gold = items[1]
    if gold == pred:
        correct += 1
    total += 1
print("Acc: ", correct * 1.0 / total)
