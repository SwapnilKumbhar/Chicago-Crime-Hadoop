import matplotlib.pyplot as plt

class Plotter:
    def __init__(self, config):
        self.filename = config['filename']
        self.tmp = config['tmp']

    def plot(self):
        data = open(self.tmp + self.filename, 'r')

        labels = []
        nums = []

        while True:
            x = data.readline()
            if not x:
                break
            x = x.rstrip().split('\t')
            labels.append(x[0])
            nums.append(int(x[1]))

        plt.bar(labels, nums)
        plt.xlabel('Crimes')
        plt.ylabel('Count')
        plt.xticks(rotation=90)
        plt.show()
        return 