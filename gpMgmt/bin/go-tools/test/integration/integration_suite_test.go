package integration_test

import (
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}

var _ = Describe("Integration", func() {
	var host string
	BeforeEach(func() {
		host = "localhost"
	})

	It("run gp install with host option", func() {
		cmd := exec.Command("gp", "install", "--host", host)
		_, _ = cmd.CombinedOutput()

		Expect(cmd.ProcessState.ExitCode()).To(Equal(0))
	})

	It("run gp install with host and agent-port option", func() {
		cmd := exec.Command("gp", "install", "--host", host, "agent-port", "3000")
		_, _ = cmd.CombinedOutput()

		Expect(cmd.ProcessState.ExitCode()).To(Equal(0))
	})
})
