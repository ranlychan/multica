package workflowrunner

import "testing"

func TestWorkflowGraphDigestMatchesJavaScriptUnicodeAndEscaping(t *testing.T) {
	// Independently calculated with Node crypto + JSON.stringify. UTF16 puts the
	// emoji before U+E000, and JSON.stringify does not HTML/U+2028/U+2029 escape.
	g := Graph{Steps: []Step{{ID: "😀", Parents: []string{"\ue000", "<&\u2028\u2029"}}, {ID: "\ue000"}, {ID: "<&\u2028\u2029"}}}
	if got := GraphDigest(g); got != "017749e2d68e11fc98da382b67cda3b1cb4daf7756052b14c89cb8e89df88b78" {
		t.Fatal(got)
	}
	if got := GraphDigest(Graph{Steps: []Step{{ID: "a"}}}); got != "7b724182be367fffa4fc246a58f9ead7cb1f5582012bd68f646f4e6077775a76" {
		t.Fatal(got)
	}
}
