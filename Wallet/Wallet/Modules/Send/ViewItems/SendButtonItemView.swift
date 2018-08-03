import UIKit
import GrouviActionSheet
import SnapKit

class SendButtonItemView: BaseButtonItemView {

    override var item: SendButtonItem? { return _item as? SendButtonItem }

    override func initView() {
        super.initView()

        button.cornerRadius = SendTheme.cornerRadius

        button.snp.makeConstraints { maker in
            maker.leading.equalToSuperview().offset(SendTheme.sendButtonSideMargin)
            maker.bottom.equalToSuperview().offset(-SendTheme.sendButtonBottomMargin)
            maker.trailing.equalToSuperview().offset(-SendTheme.sendButtonSideMargin)
            maker.height.equalTo(SendTheme.sendButtonHeight)
        }

        item?.updateButtonTopConstraint = { [weak self] top in
            self?.button.snp.updateConstraints { maker in
                maker.top.equalTo(top)
            }
        }
    }

}