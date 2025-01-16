// Code generated by smithy-go-codegen DO NOT EDIT.

package cloudwatchlogs

import (
	"context"
	"fmt"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Deletes a CloudWatch Logs account policy. This stops the account-wide policy
// from applying to log groups in the account. If you delete a data protection
// policy or subscription filter policy, any log-group level policies of those
// types remain in effect.
//
// To use this operation, you must be signed on with the correct permissions
// depending on the type of policy that you are deleting.
//
//   - To delete a data protection policy, you must have the
//     logs:DeleteDataProtectionPolicy and logs:DeleteAccountPolicy permissions.
//
//   - To delete a subscription filter policy, you must have the
//     logs:DeleteSubscriptionFilter and logs:DeleteAccountPolicy permissions.
//
//   - To delete a transformer policy, you must have the logs:DeleteTransformer and
//     logs:DeleteAccountPolicy permissions.
//
//   - To delete a field index policy, you must have the logs:DeleteIndexPolicy and
//     logs:DeleteAccountPolicy permissions.
//
// If you delete a field index policy, the indexing of the log events that
// happened before you deleted the policy will still be used for up to 30 days to
// improve CloudWatch Logs Insights queries.
func (c *Client) DeleteAccountPolicy(ctx context.Context, params *DeleteAccountPolicyInput, optFns ...func(*Options)) (*DeleteAccountPolicyOutput, error) {
	if params == nil {
		params = &DeleteAccountPolicyInput{}
	}

	result, metadata, err := c.invokeOperation(ctx, "DeleteAccountPolicy", params, optFns, c.addOperationDeleteAccountPolicyMiddlewares)
	if err != nil {
		return nil, err
	}

	out := result.(*DeleteAccountPolicyOutput)
	out.ResultMetadata = metadata
	return out, nil
}

type DeleteAccountPolicyInput struct {

	// The name of the policy to delete.
	//
	// This member is required.
	PolicyName *string

	// The type of policy to delete.
	//
	// This member is required.
	PolicyType types.PolicyType

	noSmithyDocumentSerde
}

type DeleteAccountPolicyOutput struct {
	// Metadata pertaining to the operation's result.
	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}

func (c *Client) addOperationDeleteAccountPolicyMiddlewares(stack *middleware.Stack, options Options) (err error) {
	if err := stack.Serialize.Add(&setOperationInputMiddleware{}, middleware.After); err != nil {
		return err
	}
	err = stack.Serialize.Add(&awsAwsjson11_serializeOpDeleteAccountPolicy{}, middleware.After)
	if err != nil {
		return err
	}
	err = stack.Deserialize.Add(&awsAwsjson11_deserializeOpDeleteAccountPolicy{}, middleware.After)
	if err != nil {
		return err
	}
	if err := addProtocolFinalizerMiddlewares(stack, options, "DeleteAccountPolicy"); err != nil {
		return fmt.Errorf("add protocol finalizers: %v", err)
	}

	if err = addlegacyEndpointContextSetter(stack, options); err != nil {
		return err
	}
	if err = addSetLoggerMiddleware(stack, options); err != nil {
		return err
	}
	if err = addClientRequestID(stack); err != nil {
		return err
	}
	if err = addComputeContentLength(stack); err != nil {
		return err
	}
	if err = addResolveEndpointMiddleware(stack, options); err != nil {
		return err
	}
	if err = addComputePayloadSHA256(stack); err != nil {
		return err
	}
	if err = addRetry(stack, options); err != nil {
		return err
	}
	if err = addRawResponseToMetadata(stack); err != nil {
		return err
	}
	if err = addRecordResponseTiming(stack); err != nil {
		return err
	}
	if err = addSpanRetryLoop(stack, options); err != nil {
		return err
	}
	if err = addClientUserAgent(stack, options); err != nil {
		return err
	}
	if err = smithyhttp.AddErrorCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = smithyhttp.AddCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = addSetLegacyContextSigningOptionsMiddleware(stack); err != nil {
		return err
	}
	if err = addTimeOffsetBuild(stack, c); err != nil {
		return err
	}
	if err = addUserAgentRetryMode(stack, options); err != nil {
		return err
	}
	if err = addOpDeleteAccountPolicyValidationMiddleware(stack); err != nil {
		return err
	}
	if err = stack.Initialize.Add(newServiceMetadataMiddleware_opDeleteAccountPolicy(options.Region), middleware.Before); err != nil {
		return err
	}
	if err = addRecursionDetection(stack); err != nil {
		return err
	}
	if err = addRequestIDRetrieverMiddleware(stack); err != nil {
		return err
	}
	if err = addResponseErrorMiddleware(stack); err != nil {
		return err
	}
	if err = addRequestResponseLogging(stack, options); err != nil {
		return err
	}
	if err = addDisableHTTPSMiddleware(stack, options); err != nil {
		return err
	}
	if err = addSpanInitializeStart(stack); err != nil {
		return err
	}
	if err = addSpanInitializeEnd(stack); err != nil {
		return err
	}
	if err = addSpanBuildRequestStart(stack); err != nil {
		return err
	}
	if err = addSpanBuildRequestEnd(stack); err != nil {
		return err
	}
	return nil
}

func newServiceMetadataMiddleware_opDeleteAccountPolicy(region string) *awsmiddleware.RegisterServiceMetadata {
	return &awsmiddleware.RegisterServiceMetadata{
		Region:        region,
		ServiceID:     ServiceID,
		OperationName: "DeleteAccountPolicy",
	}
}
