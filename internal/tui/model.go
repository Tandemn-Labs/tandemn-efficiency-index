package tui

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/tandemn-labs/tandemn-efficiency-index/internal/control"
)

var (
	ink        = lipgloss.Color("#F8FAFC")
	black      = lipgloss.Color("#000000")
	gray       = lipgloss.Color("#94A3B8")
	blue       = lipgloss.Color("#00D9E8")
	blueStrong = lipgloss.Color("#67E8F9")
	blueSoft   = lipgloss.Color("#083344")
	surface    = lipgloss.Color("#0F172A")
	canvas     = lipgloss.Color("#020617")
	border     = lipgloss.Color("#334155")
	clay       = blue
	cream      = ink
	muted      = gray
	green      = blueStrong
	red        = ink
	amber      = gray
	panel      = blueSoft
	logoStyle  = lipgloss.NewStyle().
			Bold(true).
			Foreground(blue)
	mutedStyle   = lipgloss.NewStyle().Foreground(muted)
	goodStyle    = lipgloss.NewStyle().Foreground(green)
	badStyle     = lipgloss.NewStyle().Foreground(red).Bold(true)
	warnStyle    = lipgloss.NewStyle().Foreground(amber)
	commandStyle = lipgloss.NewStyle().
			Foreground(blue).
			Bold(true)
	selectedStyle = lipgloss.NewStyle().
			Background(blue).
			Foreground(black).
			Bold(true)
	assistantStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(border).
			Background(surface).
			Padding(0, 1)
	userStyle = lipgloss.NewStyle().
			Foreground(blueStrong).
			Background(panel).
			Padding(0, 1)
)

type transcriptEntry struct {
	command string
	content string
	isError bool
}

type commandResult struct {
	command           string
	payload           control.Response
	err               error
	initial           bool
	connectionChecked bool
}

type connectionState string

const (
	connectionConnecting   connectionState = "connecting"
	connectionConnected    connectionState = "connected"
	connectionDegraded     connectionState = "degraded"
	connectionDisconnected connectionState = "disconnected"
)

type commandSpec struct {
	name        string
	description string
}

var availableCommands = []commandSpec{
	{name: "status", description: "Lifecycle and dependency state"},
	{name: "health", description: "API health"},
	{name: "ready", description: "Collection readiness"},
	{name: "resources", description: "Available Kubernetes resource map"},
	{name: "workloads", description: "Observed jobs and coverage"},
	{name: "snapshot", description: "Report summary for a time window"},
	{name: "start", description: "Resume collection"},
	{name: "stop", description: "Pause collection"},
	{name: "restart", description: "Restart collection"},
	{name: "dashboard", description: "Open the browser dashboard"},
	{name: "clear", description: "Clear this conversation"},
	{name: "help", description: "Show command help"},
	{name: "quit", description: "Leave the control room"},
}

type model struct {
	client          *control.Client
	dashboardURL    string
	input           textinput.Model
	viewport        viewport.Model
	spinner         spinner.Model
	entries         []transcriptEntry
	history         []string
	historyIndex    int
	suggestionIndex int
	width           int
	height          int
	ready           bool
	loading         bool
	lifecycle       string
	connection      connectionState
	activeCommand   string
	pendingAction   string
	status          control.Response
	snapshot        control.Response
	workloadIndex   int
}

func Run(client *control.Client, dashboardURL string) error {
	program := tea.NewProgram(newModel(client, dashboardURL), tea.WithAltScreen())
	_, err := program.Run()
	return err
}

func newModel(client *control.Client, dashboardURL string) model {
	input := textinput.New()
	input.Prompt = "› "
	input.PromptStyle = commandStyle
	input.TextStyle = lipgloss.NewStyle().Foreground(ink)
	input.PlaceholderStyle = mutedStyle
	input.Placeholder = "Ask TEI for /status, /workloads, /snapshot…"
	input.CharLimit = 256
	input.Focus()

	activity := spinner.New()
	activity.Spinner = spinner.Dot
	activity.Style = commandStyle

	return model{
		client:       client,
		dashboardURL: dashboardURL,
		input:        input,
		viewport:     viewport.New(80, 18),
		spinner:      activity,
		entries: []transcriptEntry{
			{
				content: "Checking the TEI control plane…",
			},
		},
		historyIndex:  -1,
		loading:       true,
		lifecycle:     "connecting",
		connection:    connectionConnecting,
		activeCommand: "/status",
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(textinput.Blink, m.spinner.Tick, m.runCommand("/status", true))
}

func (m model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	var commands []tea.Cmd
	switch message := message.(type) {
	case tea.WindowSizeMsg:
		m.width = message.Width
		m.height = message.Height
		m.viewport.Width = max(30, message.Width-4)
		m.viewport.Height = max(1, message.Height-5)
		m.input.Width = max(20, message.Width-10)
		m.ready = true
		m.refreshTranscript()
	case tea.KeyMsg:
		switch message.String() {
		case "ctrl+c":
			return m, tea.Quit
		case "esc":
			if m.input.Value() != "" {
				m.input.SetValue("")
				m.suggestionIndex = 0
				m.pendingAction = ""
				return m, nil
			}
			return m, tea.Quit
		case "ctrl+l":
			m.entries = nil
			m.pendingAction = ""
			m.refreshTranscript()
			return m, nil
		case "up":
			if m.moveSuggestion(-1) {
				return m, nil
			}
			if m.input.Value() == "" && m.moveWorkload(-1) {
				return m, nil
			}
			m.previousHistory()
			return m, nil
		case "down":
			if m.moveSuggestion(1) {
				return m, nil
			}
			if m.input.Value() == "" && m.moveWorkload(1) {
				return m, nil
			}
			m.nextHistory()
			return m, nil
		case "r":
			if m.input.Value() == "" && !m.loading {
				m.entries = append(m.entries, transcriptEntry{command: "/workloads"})
				m.loading = true
				m.activeCommand = "refreshing workloads"
				m.refreshTranscript()
				return m, tea.Batch(m.spinner.Tick, m.runCommand("/workloads", false))
			}
		case "left":
			if m.moveSuggestionColumn(-1) {
				return m, nil
			}
		case "right":
			if m.moveSuggestionColumn(1) {
				return m, nil
			}
		case "tab":
			if m.completeSuggestion() {
				return m, nil
			}
		case "enter":
			raw := strings.TrimSpace(m.input.Value())
			if raw == "" || m.loading {
				return m, nil
			}
			if !isExactCommand(raw) && m.completeSuggestion() {
				return m, nil
			}
			if !strings.HasPrefix(raw, "/") {
				raw = "/" + raw
			}
			if raw == "/quit" || raw == "/exit" {
				return m, tea.Quit
			}
			if raw == "/clear" {
				m.entries = nil
				m.input.SetValue("")
				m.pendingAction = ""
				m.refreshTranscript()
				return m, nil
			}
			if err := validateCommand(raw); err != nil {
				m.entries = append(m.entries,
					transcriptEntry{command: raw},
					transcriptEntry{content: err.Error(), isError: true},
				)
				m.input.SetValue("")
				m.pendingAction = ""
				m.refreshTranscript()
				return m, nil
			}
			if needsConfirmation(raw) && m.pendingAction != raw {
				m.pendingAction = raw
				m.entries = append(m.entries, transcriptEntry{
					content: "Press Enter again to confirm " + raw + ". Esc cancels.",
				})
				m.refreshTranscript()
				return m, nil
			}
			m.pendingAction = ""
			m.history = append(m.history, raw)
			m.historyIndex = -1
			m.entries = append(m.entries, transcriptEntry{command: raw})
			m.input.SetValue("")
			m.loading = true
			m.activeCommand = raw
			m.refreshTranscript()
			return m, tea.Batch(m.spinner.Tick, m.runCommand(raw, false))
		}
	case spinner.TickMsg:
		var command tea.Cmd
		m.spinner, command = m.spinner.Update(message)
		commands = append(commands, command)
	case commandResult:
		m.loading = false
		m.activeCommand = ""
		content := ""
		if message.err != nil {
			content = message.err.Error()
			if message.connectionChecked && message.initial {
				m.connection = connectionDisconnected
			} else if message.connectionChecked {
				m.connection = connectionDegraded
			}
		} else {
			content = formatCommandResult(message.command, message.payload)
			if message.connectionChecked {
				m.connection = connectionConnected
			}
			if lifecycle, ok := message.payload["lifecycle"].(string); ok {
				m.lifecycle = lifecycle
			}
			m.updateDashboardData(message.command, message.payload)
		}
		entry := transcriptEntry{
			content: content,
			isError: message.err != nil,
		}
		if message.initial && message.command == "status" {
			m.entries = []transcriptEntry{entry}
		} else {
			m.entries = append(m.entries, entry)
		}
		m.refreshTranscript()
		if message.initial && message.command == "status" && message.err == nil {
			m.loading = true
			m.activeCommand = "loading workloads"
			return m, tea.Batch(m.spinner.Tick, m.runCommand("/workloads", true))
		}
	}

	var viewportCommand tea.Cmd
	m.viewport, viewportCommand = m.viewport.Update(message)
	commands = append(commands, viewportCommand)

	previousInput := m.input.Value()
	var inputCommand tea.Cmd
	m.input, inputCommand = m.input.Update(message)
	if m.input.Value() != previousInput {
		m.suggestionIndex = 0
	}
	commands = append(commands, inputCommand)
	return m, tea.Batch(commands...)
}

func (m model) View() string {
	if !m.ready {
		return "Connecting to TEI…"
	}

	status := m.connectionStatus()
	header := lipgloss.JoinHorizontal(
		lipgloss.Top,
		logoStyle.Render("TEI"),
		mutedStyle.Render("  CONTROL ROOM  ·  "),
		status,
	)
	header = lipgloss.NewStyle().
		Background(canvas).
		Foreground(ink).
		Width(max(20, m.width-2)).
		Padding(0, 1).
		Render(header)

	palette := m.commandPalette()
	paletteHeight := lipgloss.Height(palette)
	composer := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(border).
		Background(canvas).
		Padding(0, 1).
		Width(max(20, m.width-4)).
		Render(m.input.View())
	shortcuts := "↑↓ workloads   r refresh   / commands   ctrl+l clear   esc quit"
	if len(commandSuggestions(m.input.Value())) > 0 {
		shortcuts = "↑↓ select   ←→ columns   tab/↵ complete   esc clear"
	}
	if m.pendingAction != "" {
		shortcuts = "↵ confirm " + m.pendingAction + "   esc cancel"
	}
	activity := mutedStyle.Render("idle")
	if m.loading {
		activity = m.spinner.View() + " " + m.activeCommand
	}
	footer := mutedStyle.Render(shortcuts) + "   " + activity

	summary := m.summaryBar()
	activityHeight := 6
	mainHeight := max(5, m.height-lipgloss.Height(header)-lipgloss.Height(summary)-activityHeight-lipgloss.Height(composer)-lipgloss.Height(footer)-paletteHeight-2)
	main := m.dashboard(mainHeight)
	recent := m.recentActivity(activityHeight)

	screen := lipgloss.JoinVertical(
		lipgloss.Left,
		header,
		summary,
		main,
		recent,
		composer,
		palette,
		lipgloss.NewStyle().Padding(0, 2).Render(footer),
	)
	return lipgloss.NewStyle().
		Background(canvas).
		Foreground(ink).
		Width(max(20, m.width)).
		Height(max(10, m.height)).
		Render(screen)
}

func (m model) summaryBar() string {
	collector, _ := m.status["collector"].(map[string]any)
	summary, _ := m.snapshot["summary"].(map[string]any)
	readiness := statusWord(boolean(m.status["ready"]), "ready", "not ready")
	prometheus := statusWord(collector != nil && collector["last_prometheus_check_at"] != nil, "connected", "unavailable")
	storage := "not checked"
	if storageStatus, ok := collector["storage"].(map[string]any); ok {
		storage = statusWord(boolean(storageStatus["writable"]), "writable", "unavailable")
	}
	items := []string{
		summaryItem("COLLECTION", lifecycleStyle(text(m.status["lifecycle"], "connecting"))),
		summaryItem("READY", readiness),
		summaryItem("PROMETHEUS", prometheus),
		summaryItem("STORAGE", storage),
		summaryItem("WORKLOADS", text(summary["workload_count"], "—")),
		summaryItem("GPUS", text(summary["gpu_count"], "—")),
	}
	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(border).
		Background(surface).
		Padding(0, 1).
		Width(max(20, m.width-4)).
		Render(strings.Join(items, "   "))
}

func summaryItem(name, value string) string {
	return mutedStyle.Render(name+" ") + value
}

func statusWord(healthy bool, yes, no string) string {
	if healthy {
		return goodStyle.Render(yes)
	}
	return warnStyle.Render(no)
}

func (m model) dashboard(height int) string {
	if m.width < 76 {
		return m.workloadPanel(max(20, m.width-4), height)
	}
	workloadWidth := max(30, (m.width-5)*36/100)
	detailWidth := max(30, m.width-workloadWidth-5)
	return lipgloss.JoinHorizontal(
		lipgloss.Top,
		m.workloadPanel(workloadWidth, height),
		" ",
		m.workloadDetail(detailWidth, height),
	)
}

func (m model) workloadPanel(width, height int) string {
	jobs := m.workloads()
	lines := []string{commandStyle.Render("WORKLOADS")}
	if len(jobs) == 0 {
		lines = append(lines, mutedStyle.Render("No workloads observed yet."))
		return dashboardPanel(width, height, strings.Join(lines, "\n"))
	}
	visible := max(1, height-4)
	start := max(0, min(m.workloadIndex-visible+1, len(jobs)-visible))
	end := min(len(jobs), start+visible)
	for index := start; index < end; index++ {
		job := jobs[index]
		workload, _ := job["workload"].(map[string]any)
		coverage, _ := job["coverage"].(map[string]any)
		name := text(workload["namespace"], "default") + "/" + text(workload["name"], "unknown")
		marker := "  "
		displayName := truncate(name, max(10, width-16))
		nameStyle := commandStyle.Render(displayName)
		if index == m.workloadIndex {
			lines = append(lines, selectedStyle.Render("› "+displayName+"  "+text(coverage["status"], "unknown")))
			continue
		}
		lines = append(lines, marker+nameStyle+"  "+coverageStyle(text(coverage["status"], "unknown")))
	}
	return dashboardPanel(width, height, strings.Join(lines, "\n"))
}

func (m model) workloadDetail(width, height int) string {
	jobs := m.workloads()
	lines := []string{commandStyle.Render("WORKLOAD DETAIL")}
	if len(jobs) == 0 {
		lines = append(lines, mutedStyle.Render("Select a workload after collection completes."))
		return dashboardPanel(width, height, strings.Join(lines, "\n"))
	}
	job := jobs[m.workloadIndex]
	workload, _ := job["workload"].(map[string]any)
	workers, _ := job["workers"].([]any)
	coverage, _ := job["coverage"].(map[string]any)
	active := true
	if value, found := job["active"]; found {
		active = boolean(value)
	}
	activity := goodStyle.Render("active")
	if !active {
		activity = mutedStyle.Render("inactive")
	}
	lines = append(lines,
		commandStyle.Render(text(workload["namespace"], "default")+"/"+text(workload["name"], "unknown")),
		label("State")+activity,
		label("Runtime")+text(workload["runtime"], "unknown"),
		label("Model")+text(workload["model_id"], "unknown model"),
		label("Workers")+fmt.Sprintf("%d", len(workers)),
		label("Coverage")+coverageStyle(text(coverage["status"], "unknown")),
	)
	if issue := text(coverage["reason"], ""); issue != "" {
		lines = append(lines, label("Issue")+badStyle.Render(truncate(issue, max(16, width-18))))
	}
	lines = append(lines, "", mutedStyle.Render("Enter slash commands below for lifecycle actions."))
	return dashboardPanel(width, height, strings.Join(lines, "\n"))
}

func dashboardPanel(width, height int, content string) string {
	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(border).
		Background(surface).
		Padding(0, 1).
		Width(max(20, width)).
		Height(max(3, min(13, height-2))).
		Render(content)
}

func (m model) recentActivity(height int) string {
	lines := []string{commandStyle.Render("RECENT ACTIVITY")}
	start := max(0, len(m.entries)-3)
	for _, entry := range m.entries[start:] {
		if entry.command != "" {
			lines = append(lines, commandStyle.Render("› "+entry.command))
			continue
		}
		summary := strings.Split(entry.content, "\n")[0]
		if entry.isError {
			summary = badStyle.Render("error: ") + summary
		}
		lines = append(lines, "  "+truncate(summary, max(20, m.width-12)))
	}
	if len(lines) == 1 {
		lines = append(lines, mutedStyle.Render("No commands run in this session."))
	}
	return dashboardPanel(max(20, m.width-4), height, strings.Join(lines, "\n"))
}

func (m *model) updateDashboardData(command string, payload control.Response) {
	switch command {
	case "status", "start", "stop", "restart":
		m.status = payload
	case "workloads", "snapshot":
		m.snapshot = payload
		m.workloadIndex = min(m.workloadIndex, max(0, len(m.workloads())-1))
	}
}

func (m model) workloads() []map[string]any {
	rawJobs, _ := m.snapshot["jobs"].([]any)
	jobs := make([]map[string]any, 0, len(rawJobs))
	for _, rawJob := range rawJobs {
		if job, ok := rawJob.(map[string]any); ok {
			jobs = append(jobs, job)
		}
	}
	return jobs
}

func (m *model) moveWorkload(direction int) bool {
	jobs := m.workloads()
	if len(jobs) == 0 {
		return false
	}
	m.workloadIndex = (m.workloadIndex + direction + len(jobs)) % len(jobs)
	return true
}

func (m model) connectionStatus() string {
	switch m.connection {
	case connectionConnecting:
		return mutedStyle.Render("● connecting")
	case connectionDisconnected:
		return badStyle.Render("● offline")
	case connectionDegraded:
		return warnStyle.Render("● connection issue")
	}

	if m.lifecycle == "stopped" {
		return warnStyle.Render("● collection paused")
	}
	if m.lifecycle == "failed" || m.lifecycle == "closed" || m.lifecycle == "unknown" {
		return badStyle.Render("● " + m.lifecycle)
	}
	return goodStyle.Render("● collection " + m.lifecycle)
}

func (m model) commandPalette() string {
	suggestions := commandSuggestions(m.input.Value())
	if len(suggestions) == 0 {
		return ""
	}
	index := m.suggestionIndex
	if index >= len(suggestions) {
		index = 0
	}
	columnCount := m.paletteColumnCount(len(suggestions))
	rowCount := (len(suggestions) + columnCount - 1) / columnCount
	columnWidth := max(28, (m.width-10)/columnCount)
	rows := make([]string, 0, rowCount)
	for row := 0; row < rowCount; row++ {
		columns := make([]string, 0, columnCount)
		for column := 0; column < columnCount; column++ {
			suggestionIndex := row + column*rowCount
			if suggestionIndex >= len(suggestions) {
				columns = append(columns, strings.Repeat(" ", columnWidth))
				continue
			}
			columns = append(columns, renderSuggestion(
				suggestions[suggestionIndex],
				suggestionIndex == index,
				columnWidth,
			))
		}
		rows = append(rows, lipgloss.JoinHorizontal(lipgloss.Top, columns...))
	}
	content := mutedStyle.Render(fmt.Sprintf("COMMANDS  %d of %d", index+1, len(suggestions)))
	content += "\n" + strings.Join(rows, "\n")
	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(border).
		Background(surface).
		Padding(0, 1).
		Margin(0, 2).
		Render(content)
}

func renderSuggestion(suggestion commandSpec, selected bool, width int) string {
	marker := mutedStyle.Render("  ")
	name := commandStyle.Render("/" + suggestion.name)
	if selected {
		marker = commandStyle.Render("› ")
		name = selectedStyle.Render("/" + suggestion.name)
	}
	descriptionWidth := max(6, width-17)
	commandColumn := lipgloss.NewStyle().Width(12).Render(name)
	content := marker + commandColumn + " " + mutedStyle.Render(truncate(suggestion.description, descriptionWidth))
	return lipgloss.NewStyle().Width(width).MaxWidth(width).Render(content)
}

func truncate(value string, width int) string {
	characters := []rune(value)
	if len(characters) <= width {
		return value
	}
	if width <= 1 {
		return "…"
	}
	return string(characters[:width-1]) + "…"
}

func (m *model) moveSuggestion(direction int) bool {
	suggestions := commandSuggestions(m.input.Value())
	if len(suggestions) == 0 {
		return false
	}
	m.suggestionIndex = (m.suggestionIndex + direction + len(suggestions)) % len(suggestions)
	return true
}

func (m model) paletteColumnCount(suggestionCount int) int {
	if suggestionCount > 6 && m.width >= 72 {
		return 2
	}
	return 1
}

func (m *model) moveSuggestionColumn(direction int) bool {
	suggestions := commandSuggestions(m.input.Value())
	columnCount := m.paletteColumnCount(len(suggestions))
	if columnCount < 2 {
		return false
	}
	rowCount := (len(suggestions) + columnCount - 1) / columnCount
	row := m.suggestionIndex % rowCount
	column := m.suggestionIndex / rowCount
	next := (column+direction+columnCount)%columnCount*rowCount + row
	if next >= len(suggestions) {
		return false
	}
	m.suggestionIndex = next
	return true
}

func (m *model) completeSuggestion() bool {
	suggestions := commandSuggestions(m.input.Value())
	if len(suggestions) == 0 {
		return false
	}
	if m.suggestionIndex >= len(suggestions) {
		m.suggestionIndex = 0
	}
	m.input.SetValue("/" + suggestions[m.suggestionIndex].name)
	m.input.CursorEnd()
	return true
}

func commandSuggestions(value string) []commandSpec {
	if !strings.HasPrefix(value, "/") {
		return nil
	}
	filter := strings.TrimPrefix(strings.ToLower(value), "/")
	if strings.Contains(filter, " ") {
		return nil
	}
	prefixSuggestions := make([]commandSpec, 0, len(availableCommands))
	fuzzySuggestions := make([]commandSpec, 0, len(availableCommands))
	for _, command := range availableCommands {
		if strings.HasPrefix(command.name, filter) {
			prefixSuggestions = append(prefixSuggestions, command)
			continue
		}
		if commandMatches(command, filter) {
			fuzzySuggestions = append(fuzzySuggestions, command)
		}
	}
	if len(prefixSuggestions) > 0 {
		return prefixSuggestions
	}
	return fuzzySuggestions
}

func commandMatches(command commandSpec, filter string) bool {
	return strings.Contains(command.name, filter) ||
		strings.Contains(strings.ToLower(command.description), filter)
}

func isExactCommand(value string) bool {
	name := strings.TrimPrefix(strings.Fields(value)[0], "/")
	for _, command := range availableCommands {
		if command.name == name {
			return true
		}
	}
	return false
}

func (m model) runCommand(raw string, initial bool) tea.Cmd {
	return func() tea.Msg {
		parts := strings.Fields(raw)
		command := strings.TrimPrefix(parts[0], "/")
		var payload control.Response
		var err error
		switch command {
		case "status":
			payload, err = m.client.Status()
		case "health":
			payload, err = m.client.Health()
		case "ready":
			payload, err = m.client.Readiness()
		case "resources":
			payload, err = m.client.Resources()
		case "workloads":
			payload, err = m.client.Snapshot(3600, 2)
		case "snapshot":
			window := "1h"
			if len(parts) > 1 {
				window = parts[1]
			}
			seconds, windowErr := parseWindow(window)
			if windowErr != nil {
				err = windowErr
				break
			}
			payload, err = m.client.Snapshot(seconds, 180)
		case "start":
			payload, err = m.client.StartObservation()
		case "stop":
			payload, err = m.client.StopObservation()
		case "restart":
			payload, err = m.client.RestartObservation()
		case "dashboard":
			err = openBrowser(m.dashboardURL)
			payload = control.Response{"message": "Opened " + m.dashboardURL}
		case "help":
			payload = control.Response{"help": helpText()}
		default:
			err = fmt.Errorf("unknown command /%s · type /help", command)
		}
		return commandResult{
			command:           command,
			payload:           payload,
			err:               err,
			initial:           initial,
			connectionChecked: requiresControlPlane(command),
		}
	}
}

func (m *model) refreshTranscript() {
	followingLatest := m.viewport.AtBottom()
	width := max(28, m.viewport.Width-2)
	var blocks []string
	for _, entry := range m.entries {
		if entry.command != "" {
			blocks = append(blocks, userStyle.Render(entry.command))
			continue
		}
		style := assistantStyle.Width(width)
		if entry.isError {
			style = style.BorderForeground(red).Foreground(red)
		}
		blocks = append(blocks, style.Render(entry.content))
	}
	m.viewport.SetContent(strings.Join(blocks, "\n\n"))
	if followingLatest {
		m.viewport.GotoBottom()
	}
}

func (m *model) previousHistory() {
	if len(m.history) == 0 {
		return
	}
	if m.historyIndex < len(m.history)-1 {
		m.historyIndex++
	}
	m.input.SetValue(m.history[len(m.history)-1-m.historyIndex])
	m.input.CursorEnd()
}

func (m *model) nextHistory() {
	if m.historyIndex <= 0 {
		m.historyIndex = -1
		m.input.SetValue("")
		return
	}
	m.historyIndex--
	m.input.SetValue(m.history[len(m.history)-1-m.historyIndex])
	m.input.CursorEnd()
}

func formatCommandResult(command string, payload control.Response) string {
	switch command {
	case "status":
		return FormatStatus(payload)
	case "health":
		return formatHealth(payload)
	case "ready":
		return formatReadiness(payload)
	case "resources":
		return formatResources(payload)
	case "start", "stop", "restart":
		return formatLifecycleAction(command, payload)
	case "workloads":
		return FormatWorkloads(payload)
	case "snapshot":
		return formatSnapshot(payload)
	case "dashboard":
		return fmt.Sprint(payload["message"])
	case "help":
		return fmt.Sprint(payload["help"])
	default:
		data, _ := json.MarshalIndent(payload, "", "  ")
		return string(data)
	}
}

func FormatStatus(payload control.Response) string {
	lifecycle := text(payload["lifecycle"], "unknown")
	stateValue, found := payload["ready"]
	if !found {
		stateValue = payload["healthy"]
	}
	ready := boolean(stateValue)
	lastTick := text(payload["last_tick_completed_at"], "never")
	collector, _ := payload["collector"].(map[string]any)
	prometheus := "not checked"
	postgres := "not checked"
	if collector != nil {
		if collector["last_prometheus_check_at"] != nil {
			prometheus = "connected"
		}
		if storage, ok := collector["storage"].(map[string]any); ok && boolean(storage["writable"]) {
			postgres = "writable"
		}
	}

	state := goodStyle.Render("ready")
	if !ready {
		state = warnStyle.Render("not ready")
	}
	lines := []string{
		commandStyle.Render("Collection status"),
		label("Lifecycle") + lifecycleStyle(lifecycle),
		label("Readiness") + state,
		label("Prometheus") + prometheus,
		label("PostgreSQL") + postgres,
		label("Last collection") + displayTime(lastTick),
	}
	if issue := firstText(payload["last_transition_error"], collector["last_collection_error"], payload["last_tick_error"], collector["error"]); issue != "" {
		lines = append(lines, label("Issue")+badStyle.Render(issue))
	}
	return strings.Join(lines, "\n")
}

func formatHealth(payload control.Response) string {
	status := goodStyle.Render("healthy")
	if !boolean(payload["healthy"]) {
		status = badStyle.Render("unhealthy")
	}
	lines := []string{
		commandStyle.Render("API health"),
		label("Status") + status,
		label("Lifecycle") + lifecycleStyle(text(payload["lifecycle"], "unknown")),
	}
	if issue := text(payload["last_transition_error"], ""); issue != "" {
		lines = append(lines, label("Issue")+badStyle.Render(issue))
	}
	return strings.Join(lines, "\n")
}

func formatReadiness(payload control.Response) string {
	lines := strings.Split(FormatStatus(payload), "\n")
	lines[0] = commandStyle.Render("Collection readiness")
	return strings.Join(lines, "\n")
}

func formatLifecycleAction(command string, payload control.Response) string {
	message := map[string]string{
		"start":   "Collection resumed",
		"stop":    "Collection paused",
		"restart": "Collection restarted",
	}[command]
	return goodStyle.Render("✓ "+message) + "\n\n" + FormatStatus(payload)
}

func FormatWorkloads(payload control.Response) string {
	jobs, _ := payload["jobs"].([]any)
	if len(jobs) == 0 {
		return mutedStyle.Render("No workloads observed.")
	}
	lines := []string{commandStyle.Render(fmt.Sprintf("%d workloads", len(jobs)))}
	for _, rawJob := range jobs {
		job, _ := rawJob.(map[string]any)
		workload, _ := job["workload"].(map[string]any)
		workers, _ := job["workers"].([]any)
		coverage, _ := job["coverage"].(map[string]any)
		name := text(workload["name"], "unknown")
		namespace := text(workload["namespace"], "default")
		runtimeName := text(workload["runtime"], "unknown")
		modelName := text(workload["model_id"], "unknown model")
		coverageState := text(coverage["status"], "unknown")
		active := true
		if value, found := job["active"]; found {
			active = boolean(value)
		}
		activity := goodStyle.Render("● active")
		if !active {
			activity = mutedStyle.Render("○ inactive")
		}
		lines = append(lines, fmt.Sprintf(
			"%s  %s",
			activity,
			commandStyle.Render(namespace+"/"+name),
		))
		lines = append(lines, fmt.Sprintf(
			"   %s  ·  %s  ·  %d workers  ·  coverage: %s",
			runtimeName,
			modelName,
			len(workers),
			coverageStyle(coverageState),
		))
	}
	return strings.Join(lines, "\n")
}

func formatResources(payload control.Response) string {
	resources, _ := payload["resources"].(map[string]any)
	if len(resources) == 0 {
		return mutedStyle.Render("No supported Kubernetes resources available.")
	}

	names := make([]string, 0, len(resources))
	for name := range resources {
		names = append(names, name)
	}
	slices.Sort(names)

	lines := []string{commandStyle.Render(fmt.Sprintf("%d Kubernetes resources", len(names)))}
	for _, name := range names {
		resource, _ := resources[name].(map[string]any)
		version := text(resource["selected_version"], "unsupported version")
		servedVersions := stringList(resource["served_versions"])
		lines = append(lines, commandStyle.Render(name))
		lines = append(lines, fmt.Sprintf(
			"   %s  ·  served: %s",
			version,
			strings.Join(servedVersions, ", "),
		))
	}
	return strings.Join(lines, "\n")
}

func formatSnapshot(payload control.Response) string {
	summary, _ := payload["summary"].(map[string]any)
	return strings.Join([]string{
		commandStyle.Render("Snapshot ready"),
		label("Observation") + text(payload["observation_id"], "unknown"),
		label("Window") + displayTime(text(payload["sample_start"], "unknown")) + " → " + displayTime(text(payload["updated_at"], "unknown")),
		label("Workloads") + text(summary["workload_count"], "0"),
		label("Workers") + text(summary["worker_count"], "0"),
		label("GPUs") + text(summary["gpu_count"], "0"),
		label("Series") + text(summary["series_count"], "0"),
	}, "\n")
}

func helpText() string {
	return strings.Join([]string{
		commandStyle.Render("Control"),
		"/status      lifecycle and dependency state",
		"/health      API health",
		"/ready       collection readiness",
		"/start       resume collection",
		"/stop        pause collection",
		"/restart     restart collection",
		"",
		commandStyle.Render("Explore"),
		"/resources   available Kubernetes workload CRDs",
		"/workloads   observed jobs and coverage",
		"/snapshot 1h report summary for 15m, 1h, 6h, 24h, or all",
		"/dashboard   open the browser dashboard",
		"/clear       clear this conversation",
		"/quit        leave the control room",
		"",
		mutedStyle.Render("Use Up/Down to inspect workloads and r to refresh their telemetry."),
		mutedStyle.Render("Stopping or restarting collection requires a second Enter to confirm."),
	}, "\n")
}

func parseWindow(value string) (int, error) {
	windows := map[string]int{"15m": 900, "1h": 3600, "6h": 21600, "24h": 86400, "all": 0}
	seconds, ok := windows[strings.ToLower(value)]
	if !ok {
		return 0, fmt.Errorf("window must be 15m, 1h, 6h, 24h, or all")
	}
	return seconds, nil
}

func validateCommand(raw string) error {
	if !isExactCommand(raw) {
		return nil
	}
	parts := strings.Fields(raw)
	command := strings.TrimPrefix(parts[0], "/")
	if command == "snapshot" {
		if len(parts) > 2 {
			return fmt.Errorf("/snapshot accepts one window: 15m, 1h, 6h, 24h, or all")
		}
		if len(parts) == 2 {
			_, err := parseWindow(parts[1])
			return err
		}
		return nil
	}
	if len(parts) > 1 {
		return fmt.Errorf("/%s does not accept arguments", command)
	}
	return nil
}

func openBrowser(target string) error {
	return openURL(target)
}

func label(value string) string {
	return mutedStyle.Width(13).Render(value)
}

func lifecycleStyle(lifecycle string) string {
	switch lifecycle {
	case "running":
		return goodStyle.Render(lifecycle)
	case "stopped":
		return warnStyle.Render(lifecycle)
	case "failed", "closed", "unknown":
		return badStyle.Render(lifecycle)
	default:
		return mutedStyle.Render(lifecycle)
	}
}

func coverageStyle(coverage string) string {
	switch strings.ToLower(coverage) {
	case "complete":
		return goodStyle.Render(coverage)
	case "partial", "degraded":
		return warnStyle.Render(coverage)
	case "missing", "unavailable", "failed":
		return badStyle.Render(coverage)
	default:
		return mutedStyle.Render(coverage)
	}
}

func displayTime(value string) string {
	if value == "" || value == "never" || value == "unknown" {
		return value
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return value
	}
	return parsed.Local().Format("Jan 2 15:04:05 MST")
}

func firstText(values ...any) string {
	for _, value := range values {
		if result := text(value, ""); result != "" {
			return result
		}
	}
	return ""
}

func needsConfirmation(command string) bool {
	return command == "/stop" || command == "/restart"
}

func requiresControlPlane(command string) bool {
	switch command {
	case "status", "health", "ready", "resources", "workloads", "snapshot", "start", "stop", "restart":
		return true
	default:
		return false
	}
}

func text(value any, fallback string) string {
	if value == nil {
		return fallback
	}
	result := fmt.Sprint(value)
	if result == "" || result == "<nil>" {
		return fallback
	}
	return result
}

func boolean(value any) bool {
	result, _ := strconv.ParseBool(fmt.Sprint(value))
	return result
}

func stringList(value any) []string {
	values, _ := value.([]any)
	result := make([]string, 0, len(values))
	for _, item := range values {
		result = append(result, fmt.Sprint(item))
	}
	return result
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
