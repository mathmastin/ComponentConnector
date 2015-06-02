class IntCode:

    @staticmethod
    def is_code_valid(code):
        for i in code.ints:
            if(i < 0 or i > 9):
                return False
        return True

    @staticmethod
    def parse(str):
        ints = []
        for c in str:
            ints.append(int(float(c)))
        return IntCode(ints)

    @staticmethod
    def uidToKspc(uid):
        return IntCode.parse(uid).keyspace()

    def __init__(self, ints):
        self.ints = ints

    def keyspace(self):
        return self.ints[0]

    def hash(self):
        return ''.join(map(str, self.ints))

    def score(self):
        return reduce(lambda x,y: x+y, self.ints)

    def neighbors(self):
        nghbrs = list()
        for x in range(len(self.ints)):
            for y in range(len(self.ints)):
                if x != y:
                    nghbr = list(self.ints)
                    nghbr[x] += 1
                    nghbr[y] -= 1
                    new_code = IntCode(nghbr)
                    if(IntCode.is_code_valid(new_code)):
                        nghbrs.append(nghbr)
        return nghbrs
