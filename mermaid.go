package kvdb

import (
	"fmt"
	"os"
	"os/exec"
)

func Mermaid(b *Bucket) string {
	out := fmt.Sprintln("graph TD;")
	out += MermaidNode(b.node(b.root), "", "Tree")

	return out
}

func MermaidNode(n *Node, oldPrefix, prefix string) string {
	var output string
	var nodeType string
	if n.typ == NODE_TYPE_LEAF {
		nodeType = "L"
	} else {
		nodeType = "I"
	}

	//assign pointer id to nodetype
	//nodeType = fmt.Sprintf("%p", n)[8:]
	nodeType = "" // keep it empty for now

	if len(nodeType) > 0 {
		nodeType = fmt.Sprintf("-%s-", nodeType)
	}

	keysStr := ""
	for _, key := range n.Keys {
		keysStr += fmt.Sprintf("%s ", string(key))
	}
	output += fmt.Sprintf("%s(%s)\n", prefix, keysStr)
	if oldPrefix != "" {
		output += fmt.Sprintf("%s -%s-> %s\n", oldPrefix, nodeType, prefix)
	}

	if n.typ == NODE_TYPE_INTERNAL {
		for _, child := range n.children {
			childNode := n.bucket.node(child)
			if len(childNode.Keys) == 0 {
				panic(fmt.Sprintf("childNode.Keys is empty: %v", child))
			}
			output += MermaidNode(childNode, prefix, fmt.Sprintf("%s_%s", prefix, string(childNode.Keys[0])))
		}
	}

	return output
}

func printBucket(b *Bucket) {
	result := Mermaid(b)
	// write to file
	err := os.WriteFile("tree.mermaid", []byte(result), 0644)
	if err != nil {
		panic(err)
	}

	// execute mermaid cli
	// mmdc -i tree.mermaid -o tree.png

	cmd := exec.Command("mmdc", "-i", "tree.mermaid", "-o", "tree.svg")
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		panic(err)
	}

	// open tree.png
	cmd = exec.Command("open", "tree.svg")
	err = cmd.Run()
	if err != nil {
		panic(err)
	}
}