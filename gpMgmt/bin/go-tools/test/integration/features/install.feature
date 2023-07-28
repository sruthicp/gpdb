Feature: install gp

  Scenario: installing gp with --host option
    Given user run "gp install --host localhost"
    Then gp install should return return code 0

  Scenario: installing gp with --host and --agent-port option
    Given user run "gp install --host localhost --agent-port 6800"
    Then gp install should return return code 0
    And gp install should print "Copied gp.conf file to segment hosts"