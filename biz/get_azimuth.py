from libs.helper import *
from interval import Interval

# 匹配路线 输入角度和距离 输出名称
class GetRoute:
    def __init__(self, distance, angle):
        self.distance = distance
        self.angle = angle
        self.default_config = ConfigHelper.getDefault()
        self.route_name = {}

    def execute_to(self):
        name = ''
        self.route_name = eval(self.default_config['dis_route']['route_name'])
        RuleMap = MatchAngleRule()
        an_zoom = RuleMap.execute_rule(self.angle)
        GapMap = MatchGapRule()
        gap_zoom = GapMap.execute_rule(self.distance)
        if gap_zoom == 1:
            if an_zoom in Interval(1, 10):
                name = self.route_name[tuple((gap_zoom, an_zoom))]
        return name 

class MatchAngleRule:

    def __init__(self):
        self.rules = [
            RuleA(),
            RuleB(),
            RuleC(),
            RuleD(),
            RuleE(),
            RuleF(),
            RuleG(),
            RuleH(),
            RuleI(),
            RuleJ()
        ]

    def execute_rule(self, azimuth):
        for rule in self.rules:
            if rule.asert_rule(azimuth):
                return rule.rule_name() if rule.rule_name() else 0

class Rule:
    def rule_name(self):
        return 0

    def asert_rule(self, azimuth):
        pass



class RuleA(Rule):

    def rule_name(self):
        return 1

    def asert_rule(self, azimuth):
        return azimuth in Interval(0, 57)



class RuleB(Rule):

    def rule_name(self):
        return 2

    def asert_rule(self, azimuth):
        return azimuth in Interval(58, 65)


class RuleC(Rule):

    def rule_name(self):
        return 3

    def asert_rule(self, azimuth):
        return azimuth in Interval(66, 112)


class RuleD(Rule):

    def rule_name(self):
        return 4

    def asert_rule(self, azimuth):
        return azimuth in Interval(113, 138)


class RuleE(Rule):

    def rule_name(self):
        return 5

    def asert_rule(self, azimuth):
        return azimuth in Interval(139, 216)


class RuleF(Rule):

    def rule_name(self):
        return 6

    def asert_rule(self, azimuth):
        return azimuth in Interval(217, 233)

class RuleG(Rule):

    def rule_name(self):
        return 7

    def asert_rule(self, azimuth):
        return azimuth in Interval(234, 277)

# 佰悦
class RuleH(Rule):

    def rule_name(self):
        return 8

    def asert_rule(self, azimuth):
        return azimuth in Interval(278, 279)

class RuleI(Rule):

    def rule_name(self):
        return 9

    def asert_rule(self, azimuth):
        return azimuth in Interval(280, 321)

class RuleJ(Rule):

    def rule_name(self):
        return 10

    def asert_rule(self, azimuth):
        return azimuth in Interval(322, 360)

class MatchGapRule:

    def __init__(self):
        self.gaps = [
            GapA()
        ]

    def execute_rule(self, distance):
        for gap in self.gaps:
            if gap.asert_rule(distance):
                return gap.rule_name() if gap.rule_name() else 0

class GapRule:
    def rule_name(self):
        return 0

    def asert_rule(self, distance):
        pass



class GapA(GapRule):

    def rule_name(self):
        return 1

    def asert_rule(self, distance):
        return True
        # return distance in Interval(309, 321)



# class GapB(GapRule):
#
#     def rule_name(self):
#         return 2
#
#     def asert_rule(self, distance):
#         return distance in Interval(278, 280, lower_closed=False)
#
#
# class GapC(GapRule):
#
#     def rule_name(self):
#         return 3
#
#     def asert_rule(self, distance):
#         return distance in Interval(217, 233, lower_closed=False)
