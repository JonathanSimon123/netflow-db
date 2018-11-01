DROP TABLE IF EXISTS `netflow`;
CREATE TABLE `netflow`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `ip` varchar(80) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  `seconds_30_avg` int(11) NULL DEFAULT NULL,
  `time` varchar(60) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = latin1 COLLATE = latin1_swedish_ci ROW_FORMAT = Compact;