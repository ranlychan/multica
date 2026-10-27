package execenv

import (
	"strings"
	"testing"
)

func TestEmptyInstructionBriefDiscoversWorkflowsForEveryTaskKind(t *testing.T) {
	for name, ctx := range map[string]TaskContextForEnv{
		"issue": {}, "comment": {IssueID: "issue", TriggerCommentID: "comment"},
		"chat": {ChatSessionID: "chat"}, "quick create": {QuickCreatePrompt: "create"},
		"autopilot": {AutopilotRunID: "run"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx.AgentSkills = []SkillContextForEnv{platformSkillFixture()}
			brief := buildMetaSkillContent("codex", ctx)
			for _, pointer := range []string{"multica workflow", "references/workflows.md", "multica-platform"} {
				if !strings.Contains(brief, pointer) {
					t.Errorf("default %s brief lacks workflow discovery %q", name, pointer)
				}
			}
		})
	}
}
