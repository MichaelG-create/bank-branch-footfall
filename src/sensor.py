
class VisitorCounter:
    def __init__(self,
                 avg_visitor_count: int = 120,
                 std_pct: float = .15
                 )->None:
        """
        simulates a visitor counter
        uses random number generator to do so
        avg_visitor_count :
        """
        self.avg_visitor_count = avg_visitor_count
        self.std_pct = std_pct

    def get_visit_count(self, day, hour):
        """
        simulates a visitor count in a bank (city center)
        for a given :
            - day
            - hour
        Rules :
        avg_visitor_count : 120
        std_visitor_count : 15 % (for each avg_visitor_count for each day)
        - monday : avg_visits * 100%
        - tuesday : avg_visits * 125 %
        - wednesday : avg_visits * 130 %
        - thursday : avg_visits * 160 %
        - friday : avg_visits * 180 %
        :return:
        """
        return 0


