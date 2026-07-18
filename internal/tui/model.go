package tui

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/tandemn-labs/tandemn-efficiency-index/internal/control"
)

var (
	clay      = lipgloss.Color("#D97757")
	cream     = lipgloss.Color("#F4EFE8")
	muted     = lipgloss.Color("#88837D")
	green     = lipgloss.Color("#70B88A")
	red       = lipgloss.Color("#E06C75")
	amber     = lipgloss.Color("#E5B567")
	panel     = lipgloss.Color("#262421")
	logoStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(clay)
	mutedStyle   = lipgloss.NewStyle().Foreground(muted)
	goodStyle    = lipgloss.NewStyle().Foreground(green).Bold(true)
	badStyle     = lipgloss.NewStyle().Foreground(red).Bold(true)
	warnStyle    = lipgloss.NewStyle().Foreground(amber).Bold(true)
	commandStyle = lipgloss.NewStyle().
			Foreground(clay).
			Bold(true)
	assistantStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#55504A")).
			Padding(0, 1)
	userStyle = lipgloss.NewStyle().
			Foreground(cream).
			Background(panel).
			Padding(0, 1)
)

type transcriptEntry struct {
	command string
	content string
	isError bool
}

type commandResult struct {
	command string
	payload control.Response
	err     error
}

type commandSpec struct {
	name        string
	description string
}

var availableCommands = []commandSpec{
	{name: "status", description: "Lifecycle and dependency state"},
	{name: "health", description: "API health"},
	{name: "ready", description: "Collection readiness"},
	{name: "workloads", description: "Observed jobs and coverage"},
	{name: "snapshot", description: "Report summary for a time window"},
	{name: "start", description: "Resume reconciliation"},
	{name: "stop", description: "Pause reconciliation"},
	{name: "restart", description: "Restart reconciliation"},
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
				content: "Connected to the TEI control plane. Type /help to explore commands.",
			},
		},
		historyIndex: -1,
		loading:      true,
		lifecycle:    "connecting",
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(textinput.Blink, m.spinner.Tick, m.runCommand("/status"))
}

func (m model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	var commands []tea.Cmd
	switch message := message.(type) {
	case tea.WindowSizeMsg:
		m.width = message.Width
		m.height = message.Height
		m.viewport.Width = max(30, message.Width-4)
		m.viewport.Height = max(6, message.Height-9)
		m.input.Width = max(20, message.Width-10)
		m.ready = true
		m.refreshTranscript()
	case tea.KeyMsg:
		switch message.String() {
		case "ctrl+c", "esc":
			return m, tea.Quit
		case "ctrl+l":
			m.entries = nil
			m.refreshTranscript()
			return m, nil
		case "up":
			if m.moveSuggestion(-1) {
				return m, nil
			}
			m.previousHistory()
			return m, nil
		case "down":
			if m.moveSuggestion(1) {
				return m, nil
			}
			m.nextHistory()
			return m, nil
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
				m.refreshTranscript()
				return m, nil
			}
			m.history = append(m.history, raw)
			m.historyIndex = -1
			m.entries = append(m.entries, transcriptEntry{command: raw})
			m.input.SetValue("")
			m.loading = true
			m.refreshTranscript()
			return m, tea.Batch(m.spinner.Tick, m.runCommand(raw))
		}
	case spinner.TickMsg:
		var command tea.Cmd
		m.spinner, command = m.spinner.Update(message)
		commands = append(commands, command)
	case commandResult:
		m.loading = false
		content := ""
		if message.err != nil {
			content = message.err.Error()
		} else {
			content = formatCommandResult(message.command, message.payload)
			if lifecycle, ok := message.payload["lifecycle"].(string); ok {
				m.lifecycle = lifecycle
			}
		}
		m.entries = append(m.entries, transcriptEntry{
			content: content,
			isError: message.err != nil,
		})
		m.refreshTranscript()
	}

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

	status := goodStyle.Render("● " + m.lifecycle)
	if m.lifecycle == "stopped" {
		status = warnStyle.Render("● stopped")
	}
	if m.lifecycle == "failed" || m.lifecycle == "closed" {
		status = badStyle.Render("● " + m.lifecycle)
	}
	header := lipgloss.JoinHorizontal(
		lipgloss.Top,
		logoStyle.Render("TEI"),
		mutedStyle.Render("  CONTROL ROOM  ·  "+m.dashboardURL+"  ·  "),
		status,
	)
	header = lipgloss.NewStyle().Width(max(20, m.width-2)).Padding(0, 1).Render(header)

	activity := ""
	if m.loading {
		activity = m.spinner.View() + " working"
	} else {
		activity = mutedStyle.Render("ready")
	}
	palette := m.commandPalette()
	paletteHeight := lipgloss.Height(palette)
	m.viewport.Height = max(4, m.height-9-paletteHeight)
	composer := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#55504A")).
		Padding(0, 1).
		Width(max(20, m.width-4)).
		Render(m.input.View())
	shortcuts := "↵ run   ↑↓ history   ctrl+l clear   esc quit"
	if len(commandSuggestions(m.input.Value())) > 0 {
		shortcuts = "↑↓ select   tab/↵ complete   esc quit"
	}
	footer := mutedStyle.Render(shortcuts) + "   " + activity

	return lipgloss.JoinVertical(
		lipgloss.Left,
		header,
		m.viewport.View(),
		palette,
		composer,
		lipgloss.NewStyle().Padding(0, 2).Render(footer),
	)
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
	columnCount := 1
	if len(suggestions) > 6 && m.width >= 72 {
		columnCount = 2
	}
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
	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("#55504A")).
		Padding(0, 1).
		Margin(0, 2).
		Render(strings.Join(rows, "\n"))
}

func renderSuggestion(suggestion commandSpec, selected bool, width int) string {
	marker := "  "
	style := lipgloss.NewStyle()
	if selected {
		marker = commandStyle.Render("› ")
		style = style.Background(panel).Foreground(cream)
	}
	descriptionWidth := max(6, width-17)
	content := fmt.Sprintf(
		"%s%-12s %s",
		marker,
		commandStyle.Render("/"+suggestion.name),
		mutedStyle.Render(truncate(suggestion.description, descriptionWidth)),
	)
	return style.Width(width).MaxWidth(width).Render(content)
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
	suggestions := make([]commandSpec, 0, len(availableCommands))
	for _, command := range availableCommands {
		if strings.HasPrefix(command.name, filter) {
			suggestions = append(suggestions, command)
		}
	}
	return suggestions
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

func (m model) runCommand(raw string) tea.Cmd {
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
		return commandResult{command: command, payload: payload, err: err}
	}
}

func (m *model) refreshTranscript() {
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
	m.viewport.GotoBottom()
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
	case "status", "health", "ready", "start", "stop", "restart":
		return FormatStatus(payload)
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
	return strings.Join([]string{
		label("Lifecycle") + lifecycle,
		label("State") + state,
		label("Prometheus") + prometheus,
		label("PostgreSQL") + postgres,
		label("Last tick") + lastTick,
	}, "\n")
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
		lines = append(lines, fmt.Sprintf(
			"%s  %s  %s  ·  %s  ·  %d workers  ·  %s",
			goodStyle.Render("●"),
			commandStyle.Render(namespace+"/"+name),
			runtimeName,
			modelName,
			len(workers),
			coverageState,
		))
	}
	return strings.Join(lines, "\n")
}

func formatSnapshot(payload control.Response) string {
	summary, _ := payload["summary"].(map[string]any)
	return strings.Join([]string{
		commandStyle.Render("Snapshot ready"),
		label("Observation") + text(payload["observation_id"], "unknown"),
		label("Window") + text(payload["sample_start"], "unknown") + " → " + text(payload["updated_at"], "unknown"),
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
		"/start       resume reconciliation",
		"/stop        pause reconciliation",
		"/restart     restart reconciliation",
		"",
		commandStyle.Render("Explore"),
		"/workloads   observed jobs and coverage",
		"/snapshot 1h report summary for 15m, 1h, 6h, 24h, or all",
		"/dashboard   open the browser dashboard",
		"/clear       clear this conversation",
		"/quit        leave the control room",
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

func openBrowser(target string) error {
	return openURL(target)
}

func label(value string) string {
	return mutedStyle.Width(13).Render(value)
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
