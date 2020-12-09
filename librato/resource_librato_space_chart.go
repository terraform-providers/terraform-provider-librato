package librato

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/hashicorp/terraform/helper/hashcode"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/henrikhodne/go-librato/librato"
)

func resourceLibratoSpaceChart() *schema.Resource {
	return &schema.Resource{
		Create: resourceLibratoSpaceChartCreate,
		Read:   resourceLibratoSpaceChartRead,
		Update: resourceLibratoSpaceChartUpdate,
		Delete: resourceLibratoSpaceChartDelete,

		Schema: map[string]*schema.Schema{
			"space_id": {
				Type:     schema.TypeInt,
				Required: true,
				ForceNew: true,
			},
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"type": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"min": {
				Type:     schema.TypeFloat,
				Default:  math.NaN(),
				Optional: true,
			},
			"max": {
				Type:     schema.TypeFloat,
				Default:  math.NaN(),
				Optional: true,
			},
			"label": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"related_space": {
				Type:     schema.TypeInt,
				Optional: true,
			},
			"stream": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"metric": {
							Type:          schema.TypeString,
							Optional:      true,
							ConflictsWith: []string{"stream.composite"},
						},
						"source": {
							Type:          schema.TypeString,
							Optional:      true,
							ConflictsWith: []string{"stream.composite", "stream.tag"},
						},
						"tag": {
							Type:          schema.TypeList,
							Optional:      true,
							ConflictsWith: []string{"stream.composite", "stream.source"},
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"name": {
										Type:     schema.TypeString,
										Required: true,
									},
									"grouped": {
										Type:     schema.TypeBool,
										Optional: true,
										ConflictsWith: []string{"stream.tag.values"},
									},
									"dynamic": {
										Type:          schema.TypeBool,
										Optional:      true,
										ConflictsWith: []string{"stream.tag.values"},
									},
									"values": {
										Type:          schema.TypeList,
										Optional:      true,
										ConflictsWith: []string{"stream.tag.dynamic", "stream.tag.grouped"},
										Elem:          &schema.Schema{Type: schema.TypeString},
									},
								},
							},
						},
						"group_function": {
							Type:          schema.TypeString,
							Optional:      true,
							ConflictsWith: []string{"stream.composite"},
						},
						"composite": {
							Type:          schema.TypeString,
							Optional:      true,
							ConflictsWith: []string{"stream.metric", "stream.source", "stream.tag", "stream.group_function"},
						},
						"summary_function": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"name": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"color": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"units_short": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"units_long": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"min": {
							Type:     schema.TypeFloat,
							Default:  math.NaN(),
							Optional: true,
						},
						"max": {
							Type:     schema.TypeFloat,
							Default:  math.NaN(),
							Optional: true,
						},
						"transform_function": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"downsample_function": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"period": {
							Type:     schema.TypeInt,
							Optional: true,
						},
					},
				},
				Set: resourceLibratoSpaceChartHash,
			},
		},
	}
}

func resourceLibratoSpaceChartHash(v interface{}) int {
	var buf bytes.Buffer
	m := v.(map[string]interface{})
	buf.WriteString(fmt.Sprintf("%s-", m["name"].(string)))
	buf.WriteString(fmt.Sprintf("%s-", m["metric"].(string)))
	buf.WriteString(fmt.Sprintf("%s-", m["source"].(string)))
	buf.WriteString(fmt.Sprintf("%s-", m["composite"].(string)))
	tags := m["tag"].([]interface{})
	less := func(i, j int) bool {
		left := tags[i].(map[string]interface{})
		right := tags[j].(map[string]interface{})
		return left["name"].(string) < right["name"].(string)
	}
	sort.SliceStable(tags, less)
	for _, v := range tags {
		tag := v.(map[string]interface{})
		if name, ok := tag["name"]; ok {
			buf.WriteString(fmt.Sprintf("%s-", name.(string)))
		}
		if dynamic, ok := tag["dynamic"]; ok {
			buf.WriteString(fmt.Sprintf("%s-", dynamic.(bool)))
		}
		if grouped, ok := tag["grouped"]; ok {
			buf.WriteString(fmt.Sprintf("%s-", grouped.(bool)))
		}
		values := tag["values"].([]interface{})
		sort.SliceStable(values, func(i, j int) bool { return values[i].(string) < values[j].(string) })
		for _, tagValue := range values {
			buf.WriteString(fmt.Sprintf("%s-", tagValue.(string)))
		}
	}
	return hashcode.String(buf.String())
}

func resourceLibratoSpaceChartCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*librato.Client)

	spaceID := uint(d.Get("space_id").(int))

	spaceChart := new(librato.SpaceChart)
	if v, ok := d.GetOk("name"); ok {
		spaceChart.Name = librato.String(v.(string))
	}
	if v, ok := d.GetOk("type"); ok {
		spaceChart.Type = librato.String(v.(string))
	}
	if v, ok := d.GetOk("min"); ok {
		if math.IsNaN(v.(float64)) {
			spaceChart.Min = nil
		} else {
			spaceChart.Min = librato.Float(v.(float64))
		}
	}
	if v, ok := d.GetOk("max"); ok {
		if math.IsNaN(v.(float64)) {
			spaceChart.Max = nil
		} else {
			spaceChart.Max = librato.Float(v.(float64))
		}
	}
	if v, ok := d.GetOk("label"); ok {
		spaceChart.Label = librato.String(v.(string))
	}
	if v, ok := d.GetOk("related_space"); ok {
		spaceChart.RelatedSpace = librato.Uint(uint(v.(int)))
	}
	if v, ok := d.GetOk("stream"); ok {
		vs := v.(*schema.Set)
		streams := make([]librato.SpaceChartStream, vs.Len())
		for i, streamDataM := range vs.List() {
			streamData := streamDataM.(map[string]interface{})
			var stream librato.SpaceChartStream
			if v, ok := streamData["metric"].(string); ok && v != "" {
				stream.Metric = librato.String(v)
			}
			if v, ok := streamData["name"].(string); ok && v != "" {
				stream.Name = librato.String(v)
			}
			if v, ok := streamData["source"].(string); ok && v != "" {
				stream.Source = librato.String(v)
			}
			if v, ok := streamData["tag"]; ok {
				tagsList := v.([]interface{})
				var tags []librato.TagSet
				for _, tagsDataM := range tagsList {
					tagsData := tagsDataM.(map[string]interface{})
					var tag librato.TagSet
					if v, ok := tagsData["name"].(string); ok && v != "" {
						tag.Name = librato.String(v)
					}
					if v, ok := tagsData["grouped"].(bool); ok {
						tag.Grouped = librato.Bool(v)
					}
					if v, ok := tagsData["dynamic"].(bool); ok {
						tag.Dynamic = librato.Bool(v)
					}
					if v, ok := tagsData["values"]; ok {
						vs := v.([]interface{})
						var values []*string
						for _, v := range vs {
							values = append(values, librato.String(v.(string)))
						}
						tag.Values = values
					}
					tags = append(tags, tag)
				}
				sortTags(tags)
				stream.Tags = tags
			}
			if v, ok := streamData["composite"].(string); ok && v != "" {
				stream.Composite = librato.String(v)
			}
			if v, ok := streamData["group_function"].(string); ok && v != "" {
				stream.GroupFunction = librato.String(v)
			}
			if v, ok := streamData["summary_function"].(string); ok && v != "" {
				stream.SummaryFunction = librato.String(v)
			}
			if v, ok := streamData["transform_function"].(string); ok && v != "" {
				stream.TransformFunction = librato.String(v)
			}
			if v, ok := streamData["downsample_function"].(string); ok && v != "" {
				stream.DownsampleFunction = librato.String(v)
			}
			if v, ok := streamData["color"].(string); ok && v != "" {
				stream.Color = librato.String(v)
			}
			if v, ok := streamData["units_short"].(string); ok && v != "" {
				stream.UnitsShort = librato.String(v)
			}
			if v, ok := streamData["units_longs"].(string); ok && v != "" {
				stream.UnitsLong = librato.String(v)
			}
			if v, ok := streamData["min"].(float64); ok && !math.IsNaN(v) {
				stream.Min = librato.Float(v)
			}
			if v, ok := streamData["max"].(float64); ok && !math.IsNaN(v) {
				stream.Max = librato.Float(v)
			}
			streams[i] = stream
		}
		spaceChart.Streams = streams
	}

	spaceChartResult, _, err := client.Spaces.CreateChart(spaceID, spaceChart)
	if err != nil {
		return fmt.Errorf("Error creating Librato space chart %s: %s", *spaceChart.Name, err)
	}

	resource.Retry(1*time.Minute, func() *resource.RetryError {
		_, _, err := client.Spaces.GetChart(spaceID, *spaceChartResult.ID)
		if err != nil {
			if errResp, ok := err.(*librato.ErrorResponse); ok && errResp.Response.StatusCode == 404 {
				return resource.RetryableError(err)
			}
			return resource.NonRetryableError(err)
		}
		return nil
	})

	return resourceLibratoSpaceChartReadResult(d, spaceChartResult)
}

func resourceLibratoSpaceChartRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*librato.Client)

	spaceID := uint(d.Get("space_id").(int))

	id, err := strconv.ParseUint(d.Id(), 10, 0)
	if err != nil {
		return err
	}

	chart, _, err := client.Spaces.GetChart(spaceID, uint(id))
	if err != nil {
		if errResp, ok := err.(*librato.ErrorResponse); ok && errResp.Response.StatusCode == 404 {
			d.SetId("")
			return nil
		}
		return fmt.Errorf("Error reading Librato Space chart %s: %s", d.Id(), err)
	}

	return resourceLibratoSpaceChartReadResult(d, chart)
}

func resourceLibratoSpaceChartReadResult(d *schema.ResourceData, chart *librato.SpaceChart) error {
	d.SetId(strconv.FormatUint(uint64(*chart.ID), 10))
	if chart.Name != nil {
		if err := d.Set("name", *chart.Name); err != nil {
			return err
		}
	}
	if chart.Type != nil {
		if err := d.Set("type", *chart.Type); err != nil {
			return err
		}
	}
	if chart.Min != nil {
		if err := d.Set("min", *chart.Min); err != nil {
			return err
		}
	}
	if chart.Max != nil {
		if err := d.Set("max", *chart.Max); err != nil {
			return err
		}
	}
	if chart.Label != nil {
		if err := d.Set("label", *chart.Label); err != nil {
			return err
		}
	}
	if chart.RelatedSpace != nil {
		if err := d.Set("related_space", *chart.RelatedSpace); err != nil {
			return err
		}
	}

	streams := resourceLibratoSpaceChartStreamsGather(d, chart.Streams)
	if err := d.Set("stream", streams); err != nil {
		return err
	}

	return nil
}

func resourceLibratoSpaceChartStreamsGather(d *schema.ResourceData, streams []librato.SpaceChartStream) []map[string]interface{} {
	retStreams := make([]map[string]interface{}, 0, len(streams))
	for _, s := range streams {
		stream := make(map[string]interface{})
		if s.Name != nil {
			stream["name"] = *s.Name
		}
		if s.Metric != nil {
			stream["metric"] = *s.Metric
		}
		if s.Source != nil {
			stream["source"] = *s.Source
		}
		if s.Composite != nil {
			stream["composite"] = *s.Composite
		}
		if s.GroupFunction != nil {
			stream["group_function"] = *s.GroupFunction
		}
		if s.SummaryFunction != nil {
			stream["summary_function"] = *s.SummaryFunction
		}
		if s.TransformFunction != nil {
			stream["transform_function"] = *s.TransformFunction
		}
		if s.DownsampleFunction != nil {
			stream["downsample_function"] = *s.DownsampleFunction
		}
		if s.Color != nil {
			stream["color"] = *s.Color
		}
		if s.UnitsShort != nil {
			stream["units_short"] = *s.UnitsShort
		}
		if s.UnitsLong != nil {
			stream["units_long"] = *s.UnitsLong
		}
		if s.Min != nil {
			stream["min"] = *s.Min
		}
		if s.Max != nil {
			stream["max"] = *s.Max
		}
		if s.Tags != nil {
			var retTags []map[string]interface{}
			for _, t := range s.Tags {
				tag := make(map[string]interface{})
				tag["name"] = *t.Name
				var values []string
				for _, v := range t.Values {
					values = append(values, *v)
				}
				tag["values"] = values
				if t.Grouped != nil {
					tag["grouped"] = *t.Grouped
				}
				if t.Dynamic != nil {
					tag["dynamic"] = *t.Dynamic
				}
				retTags = append(retTags, tag)

			}
			stream["tag"] = retTags
		}
		retStreams = append(retStreams, stream)
	}

	return retStreams
}

func resourceLibratoSpaceChartUpdate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*librato.Client)

	spaceID := uint(d.Get("space_id").(int))
	chartID, err := strconv.ParseUint(d.Id(), 10, 0)
	if err != nil {
		return err
	}

	// Just to have whole object for comparison before/after update
	fullChart, _, err := client.Spaces.GetChart(spaceID, uint(chartID))
	if err != nil {
		return err
	}

	spaceChart := new(librato.SpaceChart)
	if d.HasChange("name") {
		spaceChart.Name = librato.String(d.Get("name").(string))
		fullChart.Name = spaceChart.Name
	}
	if d.HasChange("min") {
		if math.IsNaN(d.Get("min").(float64)) {
			spaceChart.Min = nil
		} else {
			spaceChart.Min = librato.Float(d.Get("min").(float64))
		}
		fullChart.Min = spaceChart.Min
	}
	if d.HasChange("max") {
		if math.IsNaN(d.Get("max").(float64)) {
			spaceChart.Max = nil
		} else {
			spaceChart.Max = librato.Float(d.Get("max").(float64))
		}
		fullChart.Max = spaceChart.Max
	}
	if d.HasChange("label") {
		spaceChart.Label = librato.String(d.Get("label").(string))
		fullChart.Label = spaceChart.Label
	}
	if d.HasChange("related_space") {
		spaceChart.RelatedSpace = librato.Uint(d.Get("related_space").(uint))
		fullChart.RelatedSpace = spaceChart.RelatedSpace
	}
	if d.HasChange("stream") {
		vs := d.Get("stream").(*schema.Set)
		streams := make([]librato.SpaceChartStream, vs.Len())
		for i, streamDataM := range vs.List() {
			streamData := streamDataM.(map[string]interface{})
			var stream librato.SpaceChartStream
			if v, ok := streamData["name"].(string); ok && v != "" {
				stream.Name = librato.String(v)
			}
			if v, ok := streamData["metric"].(string); ok && v != "" {
				stream.Metric = librato.String(v)
			}
			if v, ok := streamData["source"].(string); ok && v != "" {
				stream.Source = librato.String(v)
			}
			if v, ok := streamData["composite"].(string); ok && v != "" {
				stream.Composite = librato.String(v)
			}
			if v, ok := streamData["group_function"].(string); ok && v != "" {
				stream.GroupFunction = librato.String(v)
			}
			if v, ok := streamData["summary_function"].(string); ok && v != "" {
				stream.SummaryFunction = librato.String(v)
			}
			if v, ok := streamData["transform_function"].(string); ok && v != "" {
				stream.TransformFunction = librato.String(v)
			}
			if v, ok := streamData["color"].(string); ok && v != "" {
				stream.Color = librato.String(v)
			}
			if v, ok := streamData["units_short"].(string); ok && v != "" {
				stream.UnitsShort = librato.String(v)
			}
			if v, ok := streamData["units_longs"].(string); ok && v != "" {
				stream.UnitsLong = librato.String(v)
			}
			if v, ok := streamData["min"].(float64); ok && !math.IsNaN(v) {
				stream.Min = librato.Float(v)
			}
			if v, ok := streamData["max"].(float64); ok && !math.IsNaN(v) {
				stream.Max = librato.Float(v)
			}
			if v, ok := streamData["tag"]; ok {
				tagsList := v.([]interface{})
				var tags []librato.TagSet
				for _, tagsDataM := range tagsList {
					tagsData := tagsDataM.(map[string]interface{})
					var tag librato.TagSet
					if v, ok := tagsData["name"].(string); ok && v != "" {
						tag.Name = librato.String(v)
					}
					if v, ok := tagsData["grouped"].(bool); ok {
						tag.Grouped = librato.Bool(v)
					}
					if v, ok := tagsData["dynamic"].(bool); ok {
						tag.Dynamic = librato.Bool(v)
					}
					if v, ok := tagsData["values"]; ok {
						vs := v.([]interface{})
						var values []*string
						for _, v := range vs {
							values = append(values, librato.String(v.(string)))
						}
						tag.Values = values
					}
					tags = append(tags, tag)
				}
				sortTags(tags)
				stream.Tags = tags
			}
			streams[i] = stream
		}
		spaceChart.Streams = streams
		fullChart.Streams = streams
	}

	_, err = client.Spaces.UpdateChart(spaceID, uint(chartID), spaceChart)
	if err != nil {
		return fmt.Errorf("Error updating Librato space chart %s: %s", *spaceChart.Name, err)
	}

	// Wait for propagation since Librato updates are eventually consistent
	wait := resource.StateChangeConf{
		Pending:                   []string{fmt.Sprintf("%t", false)},
		Target:                    []string{fmt.Sprintf("%t", true)},
		Timeout:                   5 * time.Minute,
		MinTimeout:                2 * time.Second,
		ContinuousTargetOccurence: 5,
		Refresh: func() (interface{}, string, error) {
			log.Printf("[DEBUG] Checking if Librato Space Chart %d was updated yet", chartID)
			changedChart, _, getErr := client.Spaces.GetChart(spaceID, uint(chartID))
			if getErr != nil {
				return changedChart, "", getErr
			}

			// When one stream is updated it became last in API output. Hence order of streams
			// become different. DeepEqual will return false in this case. That is why we sort
			// streams based on metric and name here.
			var streams []librato.SpaceChartStream
			streamLess := func(i, j int) bool {
				if *streams[i].Metric != *streams[j].Metric {
					return *streams[i].Metric < *streams[j].Metric
				} else {
					return *streams[i].Name < *streams[j].Name
				}
			}
			streams = changedChart.Streams
			sort.Slice(streams, streamLess)
			streams = fullChart.Streams
			sort.Slice(streams, streamLess)

			isEqual := reflect.DeepEqual(*fullChart, *changedChart)
			log.Printf("[DEBUG] Updated Librato Space Chart %d match: %t", chartID, isEqual)
			return changedChart, fmt.Sprintf("%t", isEqual), nil
		},
	}

	_, err = wait.WaitForState()
	if err != nil {
		return fmt.Errorf("Failed updating Librato Space Chart %d: %s", chartID, err)
	}

	return resourceLibratoSpaceChartRead(d, meta)
}

func sortTags(tags []librato.TagSet) {
	sort.Slice(tags, func(i, j int) bool { return *(tags[i].Name) < *(tags[j].Name) })
	for _, v := range tags {
		tag := v
		values := tag.Values
		sort.SliceStable(values, func(i, j int) bool { return *(values[i]) < *(values[j]) })
	}

}

func resourceLibratoSpaceChartDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*librato.Client)

	spaceID := uint(d.Get("space_id").(int))

	id, err := strconv.ParseUint(d.Id(), 10, 0)
	if err != nil {
		return err
	}

	log.Printf("[INFO] Deleting Chart: %d/%d", spaceID, uint(id))
	_, err = client.Spaces.DeleteChart(spaceID, uint(id))
	if err != nil {
		return fmt.Errorf("Error deleting space: %s", err)
	}

	resource.Retry(1*time.Minute, func() *resource.RetryError {
		_, _, err := client.Spaces.GetChart(spaceID, uint(id))
		if err != nil {
			if errResp, ok := err.(*librato.ErrorResponse); ok && errResp.Response.StatusCode == 404 {
				return nil
			}
			return resource.NonRetryableError(err)
		}
		return resource.RetryableError(fmt.Errorf("space chart still exists"))
	})

	d.SetId("")
	return nil
}
