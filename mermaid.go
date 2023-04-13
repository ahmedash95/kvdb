package kvdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
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

	keysStr := make([]string, 0)
	for _, key := range n.Keys {
		keysStr = append(keysStr, string(key))
	}
	output += fmt.Sprintf("%s(%s)\n", prefix, strings.Join(keysStr, ", "))
	if oldPrefix != "" {
		output += fmt.Sprintf("%s -%s-> %s\n", oldPrefix, nodeType, prefix)
	}

	if n.typ == NODE_TYPE_INTERNAL {
		for _, child := range n.children {
			childNode := n.bucket.node(child)
			if len(childNode.Keys) == 0 { // debug only - should never happen
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

func MermaidHtml(b *Bucket) string {
	result := Mermaid(b)

	// wrap the string in "<div class="mermaid">" and "</div>"
	return fmt.Sprintf("<div class=\"mermaid active\">\n%s\n</div>", result)
}

func mermaidToHtml(arr []string) {
	content, err := ioutil.ReadFile("mermaid.html")
	if err != nil {
		panic(err)
	}

	htmlString := string(content)
	startTag := `<section id="list">`
	endTag := `</section>`

	startIdx := strings.Index(htmlString, startTag)
	endIdx := strings.Index(htmlString, endTag)

	if startIdx == -1 || endIdx == -1 {
		panic(fmt.Sprintf("section with ID 'list' not found"))
	}

	startIdx += len(startTag)
	newContent := strings.Join(arr, "\n")

	newHTMLString := htmlString[:startIdx] + newContent + htmlString[endIdx:]

	err = os.WriteFile("mermaid.html", []byte(newHTMLString), os.ModePerm)
	if err != nil {
		panic(err)
	}
}
