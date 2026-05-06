package services

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type NotifierServiceTelegramImpl struct {
	httpClient				*http.Client
	telegramBotToken		string
	telegramChatIds			[]string
	deviceName				string
}

func NewNotifierServiceTelegramImpl(telegramBotToken string, telegramChatIds []string, deviceName string) NotifierService {
	return &NotifierServiceTelegramImpl{
		httpClient: &http.Client{},
		telegramBotToken: telegramBotToken,
		telegramChatIds: telegramChatIds,
		deviceName: deviceName,
	}
}

func (s *NotifierServiceTelegramImpl) SendNotification(newIp string) error {
	msg := fmt.Sprintf(
		"设备IP变动 %s\n",
		newIp,
	)

	var wg sync.WaitGroup
	errChan := make(chan error, len(s.telegramChatIds))
	for _, v := range s.telegramChatIds {
		wg.Add(1)
		go func(v string) {
			defer wg.Done()
			reqUrl := fmt.Sprintf("https://tg.987678.xyz/bot%s/sendMessage?chat_id=%s&text=%s", s.telegramBotToken, v, url.QueryEscape(msg))
			ctx, cancel := context.WithTimeout(context.Background(), 30 * time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(
				ctx,
				"GET",
				reqUrl,
				nil,
			)
			if err != nil {
				errChan <- err
				return
			}

			resp, err := s.httpClient.Do(req)
			if err != nil {
				errChan <- err
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				slog.Error(fmt.Sprintf("could not notify chatId: '%s'", v))
			}
		}(v)
	}

	wg.Wait()
	close(errChan)
	for err := range errChan {
		if err != nil {
			err = errors.Join(err, errors.New("could not make GET requet"))
			return err
		}
	}

	return nil
}
