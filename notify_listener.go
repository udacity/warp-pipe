package warppipe

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"

	"github.com/perangel/warp-pipe/internal/store"
)

// NotifyOption is a NotifyListener option function
type NotifyOption func(*NotifyListener)

// StartFromOffset is an option for setting the startFromOffset
func StartFromOffset(offset int64) NotifyOption {
	return func(l *NotifyListener) {
		l.startFromOffset = &offset
	}
}

// StartFromTimestamp is an option for setting the startFromTimestamp
func StartFromTimestamp(t time.Time) NotifyOption {
	return func(l *NotifyListener) {
		l.startFromTimestamp = &t
	}
}

// NotifyLogger is an option for setting the logger
func NotifyLogger(logger *logrus.Logger) NotifyOption {
	return func(l *NotifyListener) {
		l.logger = logger.WithFields(logrus.Fields{"component": "listener"})
	}
}

// NotifyListener is a listener that uses Postgres' LISTEN/NOTIFY pattern for
// subscribing for subscribing to changeset enqueued in a changesets table.
// For more details see `pkg/schema/changesets`.
type NotifyListener struct {
	conn                   *pgx.Conn
	logger                 *log.Entry
	store                  store.EventStore
	startFromOffset        *int64
	startFromTimestamp     *time.Time
	lastProcessedTimestamp *time.Time
	lastProcessedChangeset *Changeset
	changesetsCh           chan *Changeset
	errCh                  chan error
	listenerLock           sync.Mutex
	orderedEvents          map[int64]*store.Event
	countOutOfOrder        int
}

// NewNotifyListener returns a new NotifyListener.
func NewNotifyListener(opts ...NotifyOption) *NotifyListener {
	l := &NotifyListener{
		changesetsCh:  make(chan *Changeset),
		errCh:         make(chan error),
		orderedEvents: make(map[int64]*store.Event),
	}

	for _, opt := range opts {
		opt(l)
	}

	if l.logger == nil {
		l.logger = logrus.WithFields(log.Fields{"component": "listener"})
	}

	return l
}

// Dial connects to the source database.
func (l *NotifyListener) Dial(connConfig *pgx.ConnConfig) error {
	conn, err := pgx.Connect(*connConfig)
	if err != nil {
		log.WithError(err).Error("Failed to connect to database.")
		return err
	}

	l.conn = conn
	return nil
}

// ListenForChanges returns a channel that emits database changesets.
func (l *NotifyListener) ListenForChanges(ctx context.Context) (chan *Changeset, chan error) {
	l.logger.Info("Starting notify listener for `warp_pipe_new_changeset`")
	l.store = store.NewChangesetStore(l.conn)

	err := loadColumnTypesPGX(l.conn)
	if err != nil {
		l.errCh <- fmt.Errorf("failed to load the column types: %w", err)
	}

	// NOTE: We start the listener here, which will begin buffering any notifications
	err = l.conn.Listen("warp_pipe_new_changeset")
	if err != nil {
		l.errCh <- fmt.Errorf("failed to listen on notify channel: %w", err)
	}

	go func() {
		if l.startFromOffset != nil {
			eventCh := make(chan *store.Event)
			doneCh := make(chan bool)
			errCh := make(chan error)

			go l.store.GetFromOffset(ctx, *l.startFromOffset, eventCh, doneCh, errCh)

		processIDLoop:
			for {
				select {
				case c := <-eventCh:
					l.logger.Debugf("processIDLoop: changeset %d", c.ID)
					l.handleChangeset(c)
				case err := <-errCh:
					log.WithError(err).Fatal("encountered an error while reading changesets")
					l.errCh <- err
				case <-doneCh:
					close(errCh)
					close(eventCh)
					break processIDLoop
				}
			}
		} else if l.startFromTimestamp != nil {
			eventCh := make(chan *store.Event)
			doneCh := make(chan bool)
			errCh := make(chan error)

			go l.store.GetSinceTimestamp(ctx, *l.startFromTimestamp, eventCh, doneCh, errCh)

		processTimestampLoop:
			for {
				select {
				case c := <-eventCh:
					l.logger.Debugf("processTimestampLoop: changeset %d", c.ID)
					l.handleChangeset(c)
				case err := <-errCh:
					log.WithError(err).Fatal("encountered an error while reading changesets")
					l.errCh <- err
				case <-doneCh:
					close(errCh)
					close(eventCh)
					break processTimestampLoop
				}
			}
		}

		// loop - listen for notifications
		for {
			msg, err := l.conn.WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					log.Info("shutting down...")
					return
				}
				if err != nil {
					log.WithError(err).Error("encountered an error while waiting for notifications")
					l.errCh <- err
				}
			}

			l.processMessage(msg)
		}
	}()

	return l.changesetsCh, l.errCh
}

func (l *NotifyListener) processMessage(msg *pgx.Notification) {
	// payload is <event_id>_<timestamp>
	parts := strings.Split(msg.Payload, "_")
	eventID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		l.logger.WithError(err).WithField("changeset_id", parts[0]).
			Error("failed to parse changeset ID from notification payload")
		l.errCh <- err
	}

	event, err := l.store.GetByID(context.Background(), eventID)
	if err != nil {
		l.logger.WithError(err).WithField("changeset_id", parts[0]).Error("failed to get changeset from store")
		l.errCh <- err
	}

	l.logger.Debugf("processMessage: changeset %d", event.ID)
	l.handleChangeset(event)
}

// handleChangesets implements a strict ordered buffer queue from unordered
// input to force changesets to be processed in order.
func (l *NotifyListener) handleChangeset(event *store.Event) {
	// TODO: Is this queue a workaround for a bug? It solves the problem, but
	// seems like a bug. Why aren't these in order to start? -B

	l.listenerLock.Lock()
	defer l.listenerLock.Unlock()
	//l.logger.Infof("Current record id: %d", event.ID)

	if l.lastProcessedChangeset == nil {
		l.processChangeset(event)
		return
	}
	if event.ID == l.lastProcessedChangeset.ID {
		// This likely means that we've already processed the changeset when reading from the
		// changesets table, and the connection is playing back buffered notifications.
		// (see: `pgx.Conn.notifications`)
		l.logger.Infof("Skipping duplicate record id: %d", event.ID)
		return
	}

	nextChangesetID := l.lastProcessedChangeset.ID + 1
	l.logger.Infof("Seeking record id: %d", nextChangesetID)

	if nextChangesetID == event.ID {
		l.processChangeset(event)
		l.countOutOfOrder = 0 //reset the count
		return
	}

	// Current record is NOT next, store it.
	l.logger.Infof("Storing OUT-OF-ORDER unprocessed record id: %d", event.ID)
	l.countOutOfOrder++
	l.orderedEvents[event.ID] = event

	if l.countOutOfOrder > 100 {
		l.logger.Infof("Out of order count reached 100+, assuming changeset ID gap, seeking record id: %d", nextChangesetID)
		nextChangesetID++
	}

	for {
		nextEvent, ok := l.orderedEvents[nextChangesetID]
		if !ok {
			return
		}
		delete(l.orderedEvents, nextChangesetID)
		l.logger.Infof("Found next unprocessed record id: %d", event.ID)

		l.processChangeset(nextEvent)
		l.countOutOfOrder = 0 //reset the count
		nextChangesetID = l.lastProcessedChangeset.ID + 1
	}
}

func (l *NotifyListener) processChangeset(event *store.Event) {
	l.logger.Infof("Processing record id: %d", event.ID)

	cs := &Changeset{
		ID:        event.ID,
		Kind:      ParseChangesetKind(event.Action),
		Schema:    event.SchemaName,
		Table:     event.TableName,
		Timestamp: event.Timestamp,
	}

	table := fmt.Sprintf(`"%s"."%s"`, event.SchemaName, event.TableName)

	if event.NewValues != nil {
		var newValues map[string]interface{}
		err := json.Unmarshal(event.NewValues, &newValues)
		if err != nil {
			l.errCh <- fmt.Errorf("failed to unmarshal new values: %w", err)
		}

		var newRawValues map[string]json.RawMessage
		err = json.Unmarshal(event.NewValues, &newRawValues)
		if err != nil {
			l.errCh <- fmt.Errorf("failed to unmarshal raw new values: %w", err)
		}

		for k, v := range newValues {
			column := k
			value := v
			rawValue := newRawValues[k]
			col := changesetColumn(table, column, value, rawValue)
			cs.NewValues = append(cs.NewValues, col)
		}
	}

	if event.OldValues != nil {
		var oldValues map[string]interface{}
		err := json.Unmarshal(event.OldValues, &oldValues)
		if err != nil {
			l.errCh <- fmt.Errorf("failed to unmarshal old values: %w", err)
		}

		var oldRawValues map[string]json.RawMessage
		err = json.Unmarshal(event.OldValues, &oldRawValues)
		if err != nil {
			l.errCh <- fmt.Errorf("failed to unmarshal raw old values: %w", err)
		}

		for k, v := range oldValues {
			column := k
			value := v
			rawValue := oldRawValues[k]
			col := changesetColumn(table, column, value, rawValue)
			cs.OldValues = append(cs.OldValues, col)
		}
	}

	l.lastProcessedTimestamp = &event.Timestamp
	l.lastProcessedChangeset = cs
	l.changesetsCh <- cs
}

func changesetColumn(table, column string, value interface{}, rawValue json.RawMessage) *ChangesetColumn {
	columnType, ok := columnTypes[table][column]
	if !ok {
		panic("column type could not be determined")
	}

	if columnType == "jsonb" || columnType == "json" {
		if value != nil {
			value = rawValue
		}
	}

	return &ChangesetColumn{
		Column: column,
		Value:  value,
	}
}

// Close closes the database connection.
func (l *NotifyListener) Close() error {
	if err := l.conn.Close(); err != nil {
		log.WithError(err).Error("Error when closing database connection.")
		return err
	}

	return nil
}
