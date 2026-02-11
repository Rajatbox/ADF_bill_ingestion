package usps_easypost

import (
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/BoxTalk/carrier-bill-ingestor/commons"
	"github.com/BoxTalk/carrier-bill-ingestor/commons/recordReader"
	data "github.com/BoxTalk/carrier-bill-ingestor/internal/carrier"
	"github.com/BoxTalk/carrier-bill-ingestor/internal/carrier/usps_easypost/columns"
	"github.com/BoxTalk/carrier-bill-ingestor/internal/domain"
	"github.com/BoxTalk/carrier-bill-ingestor/internal/util"
)

type Adapter2 struct {
	recordReaderFactory recordReader.RecordReaderFactoryFn
}

func New2(recordReaderFactory recordReader.RecordReaderFactoryFn) domain.CarrierAdapter2 {
	return &Adapter2{recordReaderFactory: recordReaderFactory}
}

func (a Adapter2) GetRecords(r io.Reader) ([]string, [][]string, error) {
	rr, err := a.recordReaderFactory(r)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create reader: %w", err)
	}

	header, rows, err := rr.Read()
	if err != nil {
		return nil, nil, err
	}
	return header, rows, nil
}

func (a Adapter2) GetBills(headers []string, rows [][]string) ([]domain.BillDetails, error) {
	hidx := commons.BuildHeaderIndex(headers)
	unique := util.NewOrderedSet[domain.BillDetails]()
	for _, row := range rows {
		createdAtDate, err := date(row, hidx, columns.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to parse invoice date: %w", err)
		}
		invoiceID := a.buildInvoiceNumber(row, hidx, createdAtDate)
		warehouseZip := data.Val(row, hidx, columns.FromZip)
		accountNumber := data.Val(row, hidx, columns.CarrierAccountID)
		unique.Add(domain.BillDetails{
			InvoiceID:    invoiceID,
			InvoiceDate:  createdAtDate,
			WarehouseZip: warehouseZip,
			AccountNo:    accountNumber,
		})
	}
	return unique.Values(), nil
}

func (a Adapter2) buildInvoiceNumber(row []string, hidx map[string]int, createdAtDate time.Time) string {
	carrierAccountId := data.Val(row, hidx, columns.CarrierAccountID)
	// Format date as YYYY-MM-DD to keep invoice_number under 40 chars
	dateStr := createdAtDate.Format("2006-01-02")
	invoiceID := fmt.Sprintf("%s-%s", carrierAccountId, dateStr)
	return invoiceID
}

func (a Adapter2) BuildStagingPlan(headers []string, rows [][]string) (domain.StagingPlan, error) {
	hidx := commons.BuildHeaderIndex(headers)
	var fedexBillRecords [][]any
	for _, row := range rows {
		var err error
		fedexBillRecords, err = a.extractBillRecords(row, hidx, fedexBillRecords)
		if err != nil {
			return domain.StagingPlan{}, err
		}
	}
	plan := a.buildStagingPlan(fedexBillRecords)
	return plan, nil
}

func (a Adapter2) Validate(billDetails []domain.BillDetails, billUploadDetails domain.BillUploadDetails) []error {
	var errors []error

	for _, bill := range billDetails {
		if bill.AccountNo == "" {
			continue
		}
		if bill.AccountNo != billUploadDetails.AccountNumber {
			slog.Warn("account number mismatch: in bill=%s, given=%s", bill.AccountNo, billUploadDetails.AccountNumber)
			accountNumberErr := fmt.Errorf("account number mismatch: in bill=%s, given=%s", bill.AccountNo, billUploadDetails.AccountNumber)
			errors = append(errors, accountNumberErr)
		}
	}

	return errors
}

func (a Adapter2) extractBillRecords(rec []string, hidx map[string]int, records [][]any) ([][]any, error) {
	// Note: id is an IDENTITY column in the database, so we don't include it in the insert
	trackingCode := data.Val(rec, hidx, columns.TrackingCode)

	rate, err := data.Float(rec, hidx, columns.Rate)
	if err != nil && data.Val(rec, hidx, columns.Rate) != "" {
		return nil, fmt.Errorf("parse rate: %w", err)
	}
	labelFee, err := data.Float(rec, hidx, columns.LabelFee)
	if err != nil && data.Val(rec, hidx, columns.LabelFee) != "" {
		return nil, fmt.Errorf("parse label fee: %w", err)
	}
	postageFee, err := data.Float(rec, hidx, columns.PostageFee)
	if err != nil && data.Val(rec, hidx, columns.PostageFee) != "" {
		return nil, fmt.Errorf("parse postage fee: %w", err)
	}
	insuranceFee, err := data.Float(rec, hidx, columns.InsuranceFee)
	if err != nil && data.Val(rec, hidx, columns.InsuranceFee) != "" {
		return nil, fmt.Errorf("parse insurance fee: %w", err)
	}
	carbonOffsetFee, err := data.Float(rec, hidx, columns.CarbonOffsetFee)
	if err != nil && data.Val(rec, hidx, columns.CarbonOffsetFee) != "" {
		return nil, fmt.Errorf("parse carbon offset fee: %w", err)
	}

	weight, err := data.Float(rec, hidx, columns.Weight)
	if err != nil && data.Val(rec, hidx, columns.Weight) != "" {
		return nil, fmt.Errorf("parse weight: %w", err)
	}
	length, err := data.Float(rec, hidx, columns.Length)
	if err != nil && data.Val(rec, hidx, columns.Length) != "" {
		return nil, fmt.Errorf("parse length: %w", err)
	}
	width, err := data.Float(rec, hidx, columns.Width)
	if err != nil && data.Val(rec, hidx, columns.Width) != "" {
		return nil, fmt.Errorf("parse width: %w", err)
	}
	height, err := data.Float(rec, hidx, columns.Height)
	if err != nil && data.Val(rec, hidx, columns.Height) != "" {
		return nil, fmt.Errorf("parse height: %w", err)
	}

	fromZip := data.Val(rec, hidx, columns.FromZip)
	uspsZone, err := strconv.Atoi(data.Val(rec, hidx, columns.USPSZone))
	if err != nil && data.Val(rec, hidx, columns.USPSZone) != "" {
		return nil, fmt.Errorf("parse uspsZone: %w", err)
	}

	billDate, err := date(rec, hidx, columns.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("parse created date: %w", err)
	}

	postageLabelCreatedAt, err := date(rec, hidx, columns.PostageLabelCreatedAt)
	if err != nil {
		return nil, fmt.Errorf("parse postage label date: %w", err)
	}

	invoiceNumber := a.buildInvoiceNumber(rec, hidx, billDate)
	service := data.Val(rec, hidx, columns.Service)

	records = append(records, []any{
		trackingCode, weight,
		rate, labelFee, postageFee,
		uspsZone, fromZip,
		length, width, height,
		postageLabelCreatedAt,
		insuranceFee, carbonOffsetFee,
		billDate, invoiceNumber,
		service,
	})
	return records, nil
}

func date(record []string, idx map[string]int, header string) (time.Time, error) {
	s := data.Val(record, idx, header)
	if s == "" {
		return time.Time{}, nil
	}

	d, err := time.Parse(time.RFC3339, s)
	if err == nil {
		return d, nil
	}

	d, err = time.Parse("1/2/06", s)
	if err != nil {
		return time.Time{}, fmt.Errorf("header %q: invalid date %q", header, s)
	}

	return d, nil
}

func (a Adapter2) buildStagingPlan(records [][]any) domain.StagingPlan {
	return domain.StagingPlan{
		Batches: []domain.StagingBatch{
			{
				Name:  "usps_easypost_bill",
				Table: "elt_stage.usps_easy_post_bill",
				Cols:  columns.BillDBColumnNames,
				Rows:  records,
				Chunk: 5000,
			},
		},
		Sprocs: []domain.SprocCall{
			{
				Name:   "elt_stage.usp_SyncUSPSEasyPost",
				After:  []string{"usps_easypost_bill"},
				Params: func(b domain.BillUploadDetails) []any { return []any{} },
			},
		},
	}
}