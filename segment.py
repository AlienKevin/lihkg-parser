import pycantonese

if __name__ == "__main__":
    with open("sentences.txt", "r") as f:
        for line in f.readlines():
            print(" ".join(pycantonese.segment(line.strip())))
